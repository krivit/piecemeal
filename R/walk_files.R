walker <- function(path, file_fifo_path, tot_fifo_path = NULL) {
  file_dest <- fifo(file_fifo_path, "w+", blocking = TRUE) # Block if full.

  todo <- fastmap::faststack()
  todo$push(path)

  on.exit({
    writeLines("DONE", file_dest) # Sentinel safe because all filenames end in .rds.
    close(file_dest)
    if(tot) close(tot_dest)
  })


  if (tot <- !is.null(tot_fifo_path)) {
    tot_dest <- fifo(tot_fifo_path, "w+", blocking = FALSE)

    levels <- fastmap::faststack()
    levels$push(1L)

    dirs_listed <- integer()
    dirs_found <- integer()
    files_found <- integer()

    last_tot_files <- NA
  }

  while (!is.null(d <- todo$pop())) {
    if (tot) {
      l <- levels$pop()

      if (length(dirs_listed) < l) dirs_listed <- c(dirs_listed, 0L)
      if (length(dirs_found) < l) dirs_found <- c(dirs_found, 0L)
      if (length(files_found) < l) files_found <- c(files_found, 0L)

      dirs_listed[[l]] <- dirs_listed[[l]] + 1L
    }

    dirs <- list.dirs(d, recursive = FALSE, full.names = TRUE)
    if (nsub <- length(dirs)) {
      dirs_found[[l]] <- dirs_found[[l]] + nsub
      if (tot) {
        todo$mpush(.list = dirs)
        levels$mpush(.list = rep(l + 1L, nsub))
      }
    }

    files <- list.files(d, ".*\\.rds$", full.names = TRUE, recursive = FALSE)
    writeLines(files, file_dest)

    if (tot) {
      files_found[[l]] <- files_found[[l]] + length(files)

      tot_files <- tail(files_found, 1L)
      for (r in rev(seq_along(files_found)[-1L])) {
        tot_files <- tot_files / dirs_listed[r] * dirs_found[r - 1L] + files_found[r - 1]
      }

      if (!identical(tot_files, last_tot_files)) {
        tryCatch(writeLines(as.character(tot_files), tot_dest),
                 error = function(e) {})
        last_tot_files <- tot_files
      }
    }
  }
}

walk_files_and_db <- function(path, total = FALSE) {
  file_fifo_path <- tempfile("piecemeal_file_fifo_")
  file_dest <- fifo(file_fifo_path, "w+", blocking = TRUE)

  if (total) {
    est_fifo_path <- tempfile("piecemeal_est_fifo_")
    est_dest <- fifo(est_fifo_path, "w+", blocking = FALSE) # Just empty if not.
  } else est_fifo_path <- NULL

  walker_job <- parallel::mcparallel(walker(path, file_fifo_path, est_fifo_path))

  con <- db_connect(path)

  cleanup <- function() {
    close(file_dest)
    unlink(file_fifo_path)
    if (total) {
      close(est_dest)
      unlink(est_fifo_path)
    }
    tools::pskill(walker_job$pid)
    mccollect(walker_job)
    if (!is.null(con)) DBI::dbDisconnect(con)
  }

  n_read <- 0L
  total_files <- NA

  if (!is.null(con)) {
    seen <- fastmap::fastmap()
    db_files <- if (total) DBI::dbGetQuery(con, "SELECT count(*) FROM results")[[1]] else NA

    dbres <- DBI::dbSendQuery(con, "SELECT filename FROM results")
  } else {
    seen <- NULL
    db_files <- 0L
  }

  list(
    next_file = function() {
      if (!is.null(con) && !dbHasCompleted(dbres)) {
        dbfn <- DBI::dbFetch(dbres, 1L)$filename
        seen$set(dbfn, NULL)
        n_read <<- n_read + 1L
        return(file.path(path, ".consolidated", dbfn))
      }

      fn <- readLines(file_dest, 1L)
      if (fn == "DONE") {
        cleanup()
        return(NULL)
      } else if (endsWith(fn, ".rds")) {
        n_read <<- n_read + 1L
        return(fn)
      }
    },
    pos = function() {
      n_read
    },
    length = function() {
      if (length(total_files_read <- readLines(est_dest)))
        total_files <<- as.numeric(tail(total_files_read, 1L))
      total_files + db_files
    },
    close = function() {
      cleanup()
    }
  )
}
