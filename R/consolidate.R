#' @importFrom DBI dbConnect dbDisconnect dbExecute dbGetQuery dbExistsTable
#' @importFrom RSQLite SQLite
#' @keywords internal
#' @noRd
NULL

#' Connect to the consolidated database
#' @param outdir The output directory
#' @param mode Either "read", "write", or "edit". If "read" (default), opens the database as  read-only and returns NULL if the database doesn't exist. If "write", creates the  database and table if they don't exist and opens with write permissions. If "edit", as "write", but does not create if it doesn't exist.
#' @param autoclose automatically close the connection when the calling function exits; requires the calling function to call all subsequent [on.exit()]s with `add = TRUE`.
#' @param autolock automatically acquire the lock to be released when the calling function exits; requires the calling function to call all subsequent [on.exit()]s with `add = TRUE`.
#' @return A database connection, or NULL if mode="read" and database doesn't exist or mode="write" or "edit" and database is locked. If mode="edit" and the database does not exist, `FALSE` is returned.
#' @keywords internal
#' @noRd
db_connect <- function(outdir, mode = c("read", "write", "edit"), autoclose = TRUE, autolock = TRUE) {
  mode <- match.arg(mode)
  
  db_path <- file.path(outdir, "consolidated.db")
  
  # If mode is "read" and database doesn't exist, return NULL
  if (mode == "read" && !file.exists(db_path)) {
    return(NULL)
  }

  # Open connection with appropriate flags
  if (mode == "read") {
    # Open read-only: SQLITE_OPEN_READONLY = 0x00000001
    con <- DBI::dbConnect(RSQLite::SQLite(), db_path, flags = RSQLite::SQLITE_RO)
    dbExecute(con, "PRAGMA busy_timeout = 3600000;")
  } else {
    if (mode == "edit" && !file.exists(db_path)) return(FALSE)

    if (autolock) {
      lock_path <- file.path(outdir, ".consolidated.lock")
      lock <- filelock::lock(lock_path, timeout = 0)
      if (is.null(lock)) return(NULL)
    }

    # Open read-write, create if needed (default behavior)
    con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
    dbExecute(con, "PRAGMA busy_timeout = 3600000;")
    if (autolock) {
      # This prevents lock from being released due to going out of
      # scope unless con also goes out of scope.
      attr(con, "lock") <- lock
      do.call(on.exit, list(substitute({
        filelock::unlock(attr(con, "lock"))
        unlink(lock_path)
      }), add = TRUE),
      envir = parent.frame())
    }

    # Create table if it doesn't exist (only for write mode)
    if (!DBI::dbExistsTable(con, "results")) {
      DBI::dbExecute(con, "
        CREATE TABLE results (
          filename TEXT PRIMARY KEY,
          data BLOB NOT NULL,
          mtime REAL NOT NULL
        )
      ")
    }
  }

  if (autoclose)
    do.call(on.exit, list(substitute(DBI::dbDisconnect(con)), add = TRUE),
            envir = parent.frame())

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

#' Remove a result from the consolidated database
#' @param con Database connection
#' @param filename One or more filenames (basenames) of the results
#' @return Number of rows deleted.
#' @keywords internal
#' @noRd
db_delete_result <- function(con, filename) {
  DBI::dbExecute(con, "
    DELETE FROM results WHERE filename = ?
  ", params = list(filename))
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
    con <- db_connect(con)
    if (is.null(con)) return(empty_result)
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
  readRDS(gz_con) |> convert_old_format(filename)
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

#' Check if a filename exists in the consolidated database
#' @param con The output directory (or a database connection)
#' @param filename The filename (basename) to check
#' @return TRUE if the filename exists in the database, FALSE otherwise (or if database doesn't exist)
#' @keywords internal
#' @noRd
db_has_result <- function(con, filename) {
  # If outdir is a connection, use it directly
  if (is.character(con)) {
    con <- db_connect(con)
    if (is.null(con)) return(FALSE)
  }

  # Check if the filename exists in the database
  # Use EXISTS for better performance than COUNT(*)
  result <- DBI::dbGetQuery(con, "
    SELECT EXISTS(SELECT 1 FROM results WHERE filename = ? LIMIT 1) as file_exists
  ", params = list(basename(filename)))

  as.logical(result$file_exists[1])
}

#' Consolidate individual RDS files into the SQLite database
#' @param outdir The output directory
#' @return Number of files consolidated
#' @keywords internal
#' @noRd
consolidate_results <- function(outdir) {
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

  # Get all .rds files (not .rds.lock or .rds.tmp)
  cli_progress_message("Finding unconsolidated runs")
  files_to_consolidate <- list.files(outdir, pattern = "\\.rds$", full.names = TRUE, recursive = TRUE)
  cli_progress_done()

  if (length(files_to_consolidate) == 0) {
    cli_alert_info("Nothing to consolidate.")
    return(0)
  }

  con <- db_connect(outdir, mode = "write")
  if (is.null(con)) {
    cli_alert_info("Another process is writing to the result database.")
    cli_alert_warning("Skipping consolidation.")
    return(0)
  }

  count <- 0
  # Note: Future optimisation could batch multiple inserts in a single transaction
  # for improved performance when consolidating many files
  
  for (i in cli_progress_along(files_to_consolidate, "Consolidating files")) {
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
      
      clean_empty_dirs(outdir, dirname(file_path))
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
    cli_progress_message("Scanning individual runs")
    real_files <- files[!is_consolidated]
    real_mtimes <- file.info(real_files, extra_cols = FALSE)$mtime
    mtimes[!is_consolidated] <- as.numeric(real_mtimes)
    cli_progress_done()
  }
  
  # Get mtimes for consolidated files
  if (any(is_consolidated)) {
    cli_progress_message("Scanning consolidated runs")
    con <- db_connect(outdir)
    if (!is.null(con)) {
      consolidated_files <- files[is_consolidated]
      basenames <- basename(consolidated_files)
      
      # Create temporary table with index and filename for efficient JOIN
      DBI::dbExecute(con, "CREATE TEMP TABLE requested_files (idx INTEGER NOT NULL, filename TEXT NOT NULL)")
      
      # Insert all requested basenames with their indices in batch
      DBI::dbExecute(con, "
        INSERT INTO requested_files (idx, filename) VALUES (?, ?)
      ", params = list(seq_along(basenames), basenames))
      
      # Query mtimes using JOIN, ordered by index for optimal performance
      result <- DBI::dbGetQuery(con, "
        SELECT r.mtime
        FROM requested_files rf
        LEFT JOIN results r ON rf.filename = r.filename
        ORDER BY rf.idx
      ")
      
      # Assign results directly using the index
      mtimes[is_consolidated] <- result$mtime
      
      # Clean up temporary table
      DBI::dbExecute(con, "DROP TABLE requested_files")
    }
    cli_progress_done()
  }
  
  # Convert to POSIXct for compatibility with existing code
  as.POSIXct(mtimes, origin = "1970-01-01", tz = "UTC")
}

#' Remove empty directories and any parents
#'
#' Stop as soon as a non-empty directory (resulting in [file.remove()]
#' returning `FALSE`) or `outdir` is encountered.
#'
#' @param outdir the top-level simulation result directory.
#' @param dir_path first empty directory to attempt to delete.
#'
#' @noRd
clean_empty_dirs <- function(outdir, dir_path) {
  while (!identical(normalizePath(dir_path), normalizePath(outdir)) && suppressWarnings(file.remove(dir_path)))
    dir_path <- dirname(dir_path)
}
