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

walk_files <- function(path, total = FALSE) {
  file_fifo_path <- tempfile("piecemeal_file_fifo_")
  file_dest <- fifo(file_fifo_path, "w+", blocking = TRUE)

  if (total) {
    est_fifo_path <- tempfile("piecemeal_est_fifo_")
    est_dest <- fifo(est_fifo_path, "w+", blocking = FALSE) # Just empty if not.
  } else est_fifo_path <- NULL

  walker_job <- parallel::mcparallel(walker(path, file_fifo_path, est_fifo_path))
  done <- FALSE

  cleanup <- function() {
    if (done) return()
    done <<- TRUE
    close(file_dest)
    unlink(file_fifo_path)
    if (total) {
      close(est_dest)
      unlink(est_fifo_path)
    }
    tools::pskill(walker_job$pid)
    mccollect(walker_job)
  }

  update_length <- function() {
    if (length(total_files_read <- readLines(est_dest)))
      total_files <<- as.numeric(tail(total_files_read, 1L))
  }

  pos <- 0L
  total_files <- NA

  function(get = c("next", "position", "length"), close = FALSE) {
    if (close) {
      cleanup()
      coro::exhausted()
    } else {
      get <- match.arg(get)

      switch(get,
             "next" =
               if (done) coro::exhausted()
               else {
                 update_length() # Non-blocking FIFO, should be flushed as often as possible.
                 fn <- readLines(file_dest, 1L)
                 if (fn == "DONE") {
                   cleanup()
                   coro::exhausted()
                 } else if (endsWith(fn, ".rds")) {
                   pos <<- pos + 1L
                   fn
                 }
               },

             "position" = {
               update_length()
               pos
             },

             "length" = {
               update_length()
               total_files
             })
    }
  }
}

walk_db <- function(path, total = FALSE) {
  if (is.null(con <- db_connect(path))) {
    function(get = c("next", "position", "length"), close = FALSE) {
      get <- match.arg(get)

      switch(get,
             "next" = coro::exhausted(),
             position =,
             length = 0L)
    }} else {
       done <- FALSE
       cleanup <- function() {
         if (done) return()
         done <<- TRUE
         DBI::dbDisconnect(con)
       }

       total_files <- if (total) DBI::dbGetQuery(con, "SELECT count(*) FROM results")[[1]] else NA

       pos <- 0L
       dbres <- DBI::dbSendQuery(con, "SELECT filename FROM results")

       function(get = c("next", "position", "length"), close = FALSE) {
         if (close) {
           cleanup()
           coro::exhausted()
         } else {
           get <- match.arg(get)

           switch(get,
                  "next" =
                    if (done || dbHasCompleted(dbres)) coro::exhausted()
                    else {
                      pos <<- pos + 1L
                      DBI::dbFetch(dbres, 1L)$filename
                    },

                  "position" = {
                    pos
                  },

                  "length" = {
                    total_files
                  })
         }
       }
     }
}
