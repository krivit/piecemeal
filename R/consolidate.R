#' @importFrom DBI dbConnect dbDisconnect dbExecute dbGetQuery dbExistsTable
#' @importFrom RSQLite SQLite
#' @importFrom cli cli_progress_along
#' @keywords internal
#' @noRd
NULL

#' Connect to the consolidated database
#' @param outdir The output directory
#' @param create If TRUE (default), creates the database and table if they don't exist.
#'   If FALSE and the database doesn't exist, returns NULL.
#' @return A database connection, or NULL if create=FALSE and database doesn't exist
#' @keywords internal
#' @noRd
db_connect <- function(outdir, create = TRUE) {
  db_path <- file.path(outdir, "consolidated.db")
  
  # If create is FALSE and database doesn't exist, return NULL
  if (!create && !file.exists(db_path)) {
    return(NULL)
  }
  
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)

  # Create table if it doesn't exist
  if (!DBI::dbExistsTable(con, "results")) {
    DBI::dbExecute(con, "
      CREATE TABLE results (
        filename TEXT PRIMARY KEY,
        data BLOB NOT NULL,
        mtime REAL NOT NULL
      )
    ")
  }

  con
}

#' Store a result in the consolidated database
#' @param con Database connection
#' @param filename The filename (basename) of the result
#' @param rds_data The serialized RDS data as raw bytes
#' @param mtime The modification time (as numeric, seconds since epoch)
#' @keywords internal
#' @noRd
db_store_result <- function(con, filename, rds_data, mtime) {
  # Use list() to properly wrap the blob for RSQLite
  DBI::dbExecute(con, "
    INSERT OR REPLACE INTO results (filename, data, mtime) VALUES (?, ?, ?)
  ", params = list(filename, list(rds_data), mtime))
}

#' Retrieve a result from the consolidated database
#' @param con Database connection or outdir string. If a string, opens the database from that directory.
#' @param filename The filename (basename) of the result
#' @return The deserialized result list, or `empty_result` if not found
#' @keywords internal
#' @noRd
db_get_result <- function(con, filename) {
  # If con is a string, treat it as outdir and open the database
  if (is.character(con)) {
    outdir <- con
    con <- db_connect(outdir, create = FALSE)
    if (is.null(con)) return(empty_result)
    on.exit(DBI::dbDisconnect(con), add = TRUE)
  }
  
  result <- DBI::dbGetQuery(con, "
    SELECT data FROM results WHERE filename = ?
  ", params = list(filename))

  if (nrow(result) == 0) return(empty_result)

  # Since we store raw RDS file contents which may be compressed,
  # we use gzcon() to wrap the raw connection for decompression
  raw_con <- rawConnection(result$data[[1]])
  on.exit(close(raw_con), add = TRUE)

  gz_con <- gzcon(raw_con)
  readRDS(gz_con)
}

#' List all filenames in the consolidated database
#' @param con Database connection
#' @return Character vector of filenames
#' @keywords internal
#' @noRd
db_list_filenames <- function(con) {
  result <- DBI::dbGetQuery(con, "SELECT filename FROM results")
  result$filename
}

#' Consolidate individual RDS files into the SQLite database
#' @param outdir The output directory
#' @return Number of files consolidated
#' @keywords internal
#' @noRd
consolidate_results <- function(outdir) {
  # Use a lock file to ensure only one process consolidates at a time
  lock_path <- file.path(outdir, ".consolidate.lock")
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

  lock <- filelock::lock(lock_path, timeout = 0)
  if (is.null(lock)) {
    message("Another process is consolidating. Skipping.")
    return(0)
  }

  on.exit({
    filelock::unlock(lock)
    unlink(lock_path)
  })

  # Get all .rds files (not .rds.lock or .rds.tmp)
  files_to_consolidate <- list.files(outdir, pattern = "\\.rds$", full.names = TRUE, recursive = TRUE)

  if (length(files_to_consolidate) == 0) return(0)

  con <- db_connect(outdir)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  count <- 0
  # Note: Future optimization could batch multiple inserts in a single transaction
  # for improved performance when consolidating many files
  
  for (i in cli::cli_progress_along(files_to_consolidate, "Consolidating files")) {
    file_path <- files_to_consolidate[i]
    # Only consolidate if it's a successful run
    if (safe_readRDS(file_path)$OK) {
      # Get file info before reading
      file_mtime <- file.info(file_path)$mtime
      
      # Read raw file contents directly
      rds_data <- readBin(file_path, "raw", n = file.info(file_path)$size)

      # Store in database with modification time
      filename <- basename(file_path)
      db_store_result(con, filename, rds_data, as.numeric(file_mtime))

      # Delete the original file
      unlink(file_path)
      count <- count + 1
      
      # Remove empty directories and any parents; stop as soon as a
      # non-empty directory (resulting in file.remove() returning
      # FALSE) or outdir is encountered.
      dir_path <- dirname(file_path)
      while (dir_path != outdir && suppressWarnings(file.remove(dir_path)))
        dir_path <- dirname(dir_path)
    }
  }

  count
}

#' Read a result from either individual file or consolidated database
#' @param outdir The output directory
#' @param filename The filename (can be full path or basename)
#' @param con an optional database connection that can be reused
#' @return The result list
#' @keywords internal
#' @noRd
read_result <- function(outdir, filename, con = NULL) {
  if (grepl(".consolidated", filename, fixed = TRUE)) # If a .consolidated path,
    db_get_result(con %||% outdir, basename(filename)) # read directly from database.
  else if (file.exists(filename)) # If filename is a full path,
    safe_readRDS(filename) # use it directly.
  else  # If not found,
    db_get_result(con %||% outdir, basename(filename)) # try the database.
}

#' Get modification times for files (including consolidated ones)
#' @param outdir The output directory
#' @param files Character vector of file paths (can include .consolidated paths)
#' @return Named numeric vector of mtimes (seconds since epoch), with file paths as names
#' @keywords internal
#' @noRd
get_file_mtimes <- function(outdir, files) {
  mtimes <- numeric(length(files))
  names(mtimes) <- files
  
  # Separate real files from consolidated files
  is_consolidated <- grepl(".consolidated", files, fixed = TRUE)
  
  # Get mtimes for real files
  if (any(!is_consolidated)) {
    real_files <- files[!is_consolidated]
    real_mtimes <- file.info(real_files, extra_cols = FALSE)$mtime
    mtimes[!is_consolidated] <- as.numeric(real_mtimes)
  }
  
  # Get mtimes for consolidated files
  if (any(is_consolidated)) {
    con <- db_connect(outdir, create = FALSE)
    if (!is.null(con)) {
      on.exit(DBI::dbDisconnect(con), add = TRUE)
      
      consolidated_files <- files[is_consolidated]
      basenames <- basename(consolidated_files)
      
      # Query mtimes from database
      for (i in seq_along(basenames)) {
        result <- DBI::dbGetQuery(con, "
          SELECT mtime FROM results WHERE filename = ?
        ", params = list(basenames[i]))
        
        if (nrow(result) > 0) {
          mtimes[is_consolidated][i] <- result$mtime[1]
        }
      }
    }
  }
  
  # Convert to POSIXct for compatibility with existing code
  as.POSIXct(mtimes, origin = "1970-01-01", tz = "UTC")
}
