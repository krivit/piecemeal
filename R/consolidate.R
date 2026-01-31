#' @importFrom DBI dbConnect dbDisconnect dbWriteTable dbReadTable dbExecute dbGetQuery dbExistsTable
#' @importFrom RSQLite SQLite
#' @keywords internal
#' @noRd
NULL

# Helper to create empty result structure for missing/corrupt files
empty_result <- function() {
  list(seed = NULL, treatment = NULL, output = NULL, config = NULL, OK = FALSE)
}

#' Get the path to the consolidated database file
#' @param outdir The output directory
#' @return Path to the consolidated.db file
#' @keywords internal
#' @noRd
get_db_path <- function(outdir) {
  file.path(outdir, "consolidated.db")
}

#' Connect to the consolidated database
#' @param outdir The output directory
#' @return A database connection
#' @keywords internal
#' @noRd
db_connect <- function(outdir) {
  db_path <- get_db_path(outdir)
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)

  # Create table if it doesn't exist
  if (!DBI::dbExistsTable(con, "results")) {
    DBI::dbExecute(con, "
      CREATE TABLE results (
        filename TEXT PRIMARY KEY,
        data BLOB NOT NULL
      )
    ")
  }

  con
}

#' Store a result in the consolidated database
#' @param con Database connection
#' @param filename The filename (basename) of the result
#' @param rds_data The serialized RDS data as raw bytes
#' @keywords internal
#' @noRd
db_store_result <- function(con, filename, rds_data) {
  # Use list() to properly wrap the blob for RSQLite
  DBI::dbExecute(con, "
    INSERT OR REPLACE INTO results (filename, data) VALUES (?, ?)
  ", params = list(filename, list(rds_data)))
}

#' Retrieve a result from the consolidated database
#' @param con Database connection
#' @param filename The filename (basename) of the result
#' @return The deserialized result list, or NULL if not found
#' @keywords internal
#' @noRd
db_get_result <- function(con, filename) {
  result <- DBI::dbGetQuery(con, "
    SELECT data FROM results WHERE filename = ?
  ", params = list(filename))

  if (nrow(result) == 0) return(NULL)

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

#' Check if a file exists in the consolidated database
#' @param outdir The output directory
#' @param filename The filename (basename) to check
#' @return Logical indicating if the file exists in the database
#' @keywords internal
#' @noRd
db_has_file <- function(outdir, filename) {
  db_path <- get_db_path(outdir)
  if (!file.exists(db_path)) return(FALSE)

  con <- db_connect(outdir)
  on.exit(DBI::dbDisconnect(con))

  result <- DBI::dbGetQuery(con, "
    SELECT 1 FROM results WHERE filename = ? LIMIT 1
  ", params = list(filename))

  nrow(result) > 0
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
  all_files <- list.files(outdir, pattern = "\\.rds$", full.names = TRUE, recursive = TRUE)

  if (length(all_files) == 0) return(0)

  # Consolidate all available files
  files_to_consolidate <- all_files

  con <- db_connect(outdir)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  count <- 0
  # Note: Future optimization could batch multiple inserts in a single transaction
  # for improved performance when consolidating many files
  for (file_path in files_to_consolidate) {
    # Read the RDS file to check if it's a successful run
    result <- tryCatch(
      readRDS(file_path),
      error = function(e) NULL
    )

    # Only consolidate if it's a successful run
    if (!is.null(result) && isTRUE(result$OK)) {
      # Read raw file contents directly
      rds_data <- readBin(file_path, "raw", n = file.info(file_path)$size)

      # Store in database
      filename <- basename(file_path)
      db_store_result(con, filename, rds_data)

      # Delete the original file
      unlink(file_path)
      count <- count + 1
    }
  }

  count
}

#' Read a result from either individual file or consolidated database
#' @param outdir The output directory
#' @param filename The filename (can be full path or basename)
#' @return The result list
#' @keywords internal
#' @noRd
read_result <- function(outdir, filename) {
  # Handle .consolidated virtual paths
  if (grepl("\\.consolidated", filename)) {
    filename <- basename(filename)
    # Read directly from database
    db_path <- get_db_path(outdir)
    if (file.exists(db_path)) {
      con <- db_connect(outdir)
      on.exit(DBI::dbDisconnect(con))
      result <- db_get_result(con, filename)
      if (!is.null(result)) return(result)
    }
    return(empty_result())
  }

  # If filename is a full path, use it directly
  if (file.exists(filename)) {
    return(safe_readRDS(filename))
  }

  # Extract basename for database lookup
  base_filename <- basename(filename)

  # Try to find the file in the directory structure
  file_path <- list.files(outdir, pattern = paste0("^", base_filename, "$"),
                         full.names = TRUE, recursive = TRUE)
  if (length(file_path) > 0 && file.exists(file_path[1])) {
    return(safe_readRDS(file_path[1]))
  }

  # If not found, try the consolidated database
  db_path <- get_db_path(outdir)
  if (file.exists(db_path)) {
    con <- db_connect(outdir)
    on.exit(DBI::dbDisconnect(con))

    result <- db_get_result(con, base_filename)
    if (!is.null(result)) return(result)
  }

  # If still not found, return error structure
  empty_result()
}
