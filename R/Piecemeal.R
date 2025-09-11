#' The `Piecemeal` [`R6`] Class
#'
#' @description This class exports methods for configuring a simulation, running it, debugging failed configurations, and resuming the simulation. See [the vignette `vignette("piecemeal")`](../doc/piecemeal.html) for long worked example.
#'
#' @details A chain of `R6` method calls is used to specify the setup and the worker functions, the treatment configurations to be passed to the worker, and parallelism and other simulation settings. Then, when `$run()` is called, the cluster is started, worker nodes are initialised, and every combination of random seed and treatment configuration is passed to [clusterApplyLB()] (if parallel processing is enabled).
#'
#' On the worker nodes, the worker function is not called directly; rather, care is taken to make sure that the specified configuration and seed is not already being worked on. This makes it safe to, e.g., queue multiple jobs for the same simulation. If the configuration is available, `set.seed()` is called with the seed and then the worker function is run.
#'
#' Errors in the worker function are caught and error messages saved and returned.
#'
#' @note If no treatment is specified, the function is called with no arguments (or just `.seed`).
#' 
#' @examples
#' # Initialise, with the output directory.
#' sim <- Piecemeal$new(file.path(tempdir(), "piecemeal_demo"))
#' # Clear the previous simulation, if present.
#' sim$reset()
#'
#' # Set up a simulation:
#' sim$
#'   # for every combination of x = 1, 2 and y = 1, 3, 9, 27,
#'   factorial(x = 2^(0:1), y = 3^(0:3))$
#'   # each replicated 3 times,
#'   nrep(3)$
#'   # first load library 'rlang',
#'   setup({library(rlang)})$
#'   # then for each x, y, and seed, evaluate
#'   worker(function(x, y) {
#'     p <- x*y
#'     u <- runif(1)
#'     dbl(p = p, u = u)
#'   })$
#'   # on a cluster with two nodes.
#'   cluster(2)
#'
#' # Summarise
#' sim
#'
#' # Go!
#' sim$run()
#'
#' # Get a table with the results.
#' sim$result_df()
#'
#' # For a more involved version of this example, see vignette("piecemeal").
#'
#' @import parallel
#' @importFrom rlang hash
#' @import purrr
#' @importFrom R6 R6Class
#' @importFrom utils capture.output
#' @export
Piecemeal <- R6Class("Piecemeal",
  private = list(
    .outdir = NULL,
    .cl_setup = NULL,
    .setup = {},
    .treatments = list(),
    .worker = NULL,
    .seeds = 1L,
    .cl_vars = list(),
    .cl_var_envs = list(),
    .split = c(1L, 1L),
    .error = "auto",
    .toclean = FALSE,
    .done = function() list.files(private$.outdir, ".*\\.rds$", full.names = TRUE, recursive = TRUE),
    .doing = function() {
      list.files(private$.outdir, ".*\\.rds.lock$",
                 full.names = TRUE, recursive = TRUE) |>
        keep(is_locked)
    },
    .check_args = function(which = TRUE) {
      if(length(private$.treatments)) {
        for(i in seq_along(private$.treatments)[which]) {
          treatment <- private$.treatments[[i]]
          if(".seed" %in% names(treatment))
            stop("In treatment configuration ", i, " argument ", sQuote(".seed"), " is reserved by ", sQuote("Piecemeal"), " if you wish to provide your own seed, use a different argument name.")
          astart <- if(".seed" %in% names(formals(private$.worker))) list("", ".seed") else list("")

          tryCatch(match.call(private$.worker, as.call(c(astart, treatment))),
                   error = function(e){
                     e$message <- paste0(e$message, " in treatment configuration ", i)
                     e$call <- NULL
                     stop(e)
                   })
        }
      } else {
        astart <- if(".seed" %in% names(formals(private$.worker))) list("", ".seed") else list("")

        tryCatch(match.call(private$.worker, as.call(astart)),
                 error = function(e){
                   e$message <- paste0(e$message, " in treatment configuration ", i)
                   e$call <- NULL
                   stop(e)
                 })
      }
    }
  ),

  public = list(

    #' @description Create a new `Piecemeal` instance.
    #' @param outdir the directory to hold the partial simulation results.
    initialize = function(outdir) {
      private$.outdir <- outdir
    },

    #' @description Cluster settings for the piecemeal run.
    #' @param ... either arguments to [makeCluster()] or a single argument containing either an existing cluster or `NULL` to disable clustering.
    cluster = function(...) {
      spec <- list(...)
      if(length(spec) == 1L && (is.null(spec[[1]]) || is(spec[[1]], "cluster"))) private$.cl_setup <- spec[[1]]
      else private$.cl_setup <- spec
      invisible(self)
    },

    #' @description Specify variables to be copied from the manager node to the worker nodes' global environment. (See [parallel::clusterExport()].)
    #' @param varlist a character vector with variable names.
    #' @param envir the environment on the manager node from which to take the variables; defaults to the current environment.
    #' @param .add whether the new variables should be added to the current list (if `TRUE`, the default) or replace it (if `FALSE`).
    export_vars = function(varlist, envir = parent.frame(), .add = TRUE) {
      if(private$.error == "auto") private$.toclean <- TRUE
      if(!.add) private$.cl_vars <- private$.cl_var_envs <- list()
      if(length(eid <- which(map_lgl(private$.cl_var_envs, identical, envir))) == 0L) {
        eid <- length(private$.cl_var_envs <- c(private$.cl_var_envs, list(envir)))
        private$.cl_vars[[eid]] <- character(0)
      }
      private$.cl_vars[[eid]] <- unique(c(private$.cl_vars[[eid]], varlist))
      invisible(self)
    },

    #' @description Specify code to be run on each worker node at the start of the simulation; if running locally, it will be evaluated in the global environment.
    #' @param expr an expression; if passed, replaces the previous expression; if empty, resets it to nothing.
    setup = function(expr = {}) {
      if(private$.error == "auto") private$.toclean <- TRUE
      private$.setup <- substitute(expr)
      invisible(self)
    },

    #' @description Specify the function to be run for each treatment configuration; it will be run in the global environment.
    #' @param fun a function whose arguments are specified by `$treatments()` and `$factorial()`; if it has `.seed` as a named argument, the seed will be passed as well.
    worker = function(fun) {
      if(private$.error == "auto") private$.toclean <- TRUE
      private$.worker <- fun
      invisible(self)
    },

    #' @description Specify a list of treatment configurations to be run.
    #' @param l a list, typically of lists of arguments to be passed to the function specified by `worker`; it is recommended that these be as compact as possible, since they are [`serialize`]d and sent to the worker node for every combination of treatment configuration and random seed.
    #' @param .add whether the new treatment configurations should be added to the current list (if `TRUE`, the default) or replace it (if `FALSE`.
    treatments = function(l, .add = TRUE) {
      l <- map(l, add_hash)
      if(!.add) private$.treatments <- list()
      private$.treatments <- c(private$.treatments, l)
      invisible(self)
    },

    #' @description Specify a list of treatment configurations to be run in a factorial design.
    #' @param ... vectors or lists whose Cartesian product will added to the treatment list; it is recommended that these be as compact as possible, since they are [`serialize`]d and sent to the worker node for every combination of treatment configuration and random seed.
    #' @param .filter a function that takes the same arguments as worker and returns `FALSE` if the treatment configuration should be skipped; defaults to accepting all configurations.
    #' @param .add whether the new treatment configurations should be added to the current list (if `TRUE`, the default) or replace it (if `FALSE`.
    factorial = function(..., .filter = function(...) TRUE, .add = TRUE) {
      expand.list(...) |>
        keep(.filter) |>
        self$treatments(.add = .add)

      invisible(self)
    },

    #' @description Specify a number of replications for each treatment configuration (starts out at 1).
    #' @param nrep a positive integer giving the number of replications; the seeds will be set to `1:nrep`.
    nrep = function(nrep) {
      private$.seeds <- seq_len(nrep)
      invisible(self)
    },

    #' @description Specify the seeds to be used for each replication of each treatment configuration.
    #' @param seeds an integer vector of seeds; its length will be used to infer the number of replications.
    seeds = function(seeds) {
      private$.seeds <- seeds
      invisible(self)
    },

    #' @description Run the simulation.
    #' @param shuffle Should the treatment configurations be run in a random order (`TRUE`, the default) or in the order in which they were added (`FALSE`)?
    #' @return Invisibly, a character vector with an element for each seed and treatment configuration combination attempted, indicating its file name and status, including errors.
    run = function(shuffle = TRUE) {
      if(private$.toclean) self$clean()
      private$.check_args()

      cl <- private$.cl_setup
      if(!is.null(cl) && !is(cl, "cluster")) {
        cl <- do.call(makeCluster, cl)
        on.exit(stopCluster(cl))
      }

      if(is.null(cl)) {
        run_env <- new.env(parent = parent.env(.GlobalEnv))
        run_env$.worker <- private$.worker
        run_env$.outdir <- private$.outdir
        
        eval(private$.setup, envir = run_env)

        for(i in seq_along(private$.cl_vars))
          for(name in private$.cl_vars[[i]])
            assign(name, get(name, private$.cl_var_envs[[i]]), run_env)
      } else {
        .worker <- private$.worker
        .outdir <- private$.outdir
        clusterExport(cl, c(".worker", ".outdir"), environment())

        clusterCall(cl, eval, private$.setup, envir = .GlobalEnv)

        for(i in seq_along(private$.cl_vars)) 
          clusterExport(cl, private$.cl_vars[[i]], private$.cl_var_envs[[i]])
      }

      configs <- self$todo()
      message(sprintf("Starting %d runs. (%d already done.)", length(configs), done <- attr(configs, "done")))

      if(shuffle) configs <- configs[sample.int(length(configs))]

      statuses <- simplify2array(
        if(is.null(cl)) map(configs, run_config, error = private$.error, env = run_env, .progress = "Running")
        else clusterApplyLB(cl, configs, run_config, error = private$.error)
      )

      c(
        if(length(statuses)) statuses |> strsplit("\n", fixed = TRUE) |> map_chr(2L),
        rep("SKIPPED", done)
      ) |>
        table() |> as.data.frame() |> setNames(c("Status", "Runs")) |>
        capture.output() |> paste(collapse = "\n") |> message()

      invisible(if(length(statuses)) statuses else character(0))
    },

    #' @description List the configurations still to be run.
    #' @return A list of lists with arguments to the worker functions and worker-specific configuration settings; also an attribute `"done"` giving the number of configurations skipped because they are already done.
    todo = function() {
      configs <- expand.list(seed = private$.seeds,
                             treatment = if(length(private$.treatments)) private$.treatments
                                         else list(add_hash(list())))
      fn <- map_chr(configs, config_fn)
      done <- fn %in% basename(private$.done())
      configs <- configs[!done]
      fn <- fn[!done]

      subdirs <- map(configs, config_subdir, split = private$.split)
      configs <- Map(function(conf, fn, subdirs) c(conf, list(fn = fn, subdirs = subdirs)), configs, fn, subdirs)
      structure(configs, done = sum(done))
    },

    #' @description Scan through the results files and collate them into a list.
    #' @param n maximum number of files to load; if less than the number of results, a systematic sample is taken.
    #' @param trt_tf,out_tf functions that take the treatment configuration list and the output (if not an error) respectively, and transform them; this is helpful when, for example, the output is big and so loading all the files will run out of memory.
    #' @return A list of lists containing the contents of the result files.
    #' \describe{
    #' \item{`treatment`}{arguments passed to the worker}
    #' \item{`seed`}{the seed set just before calling the worker}
    #' \item{`output`}{value returned by the worker, or `try-error`}
    #' \item{`OK`}{whether the worker succeeded or produced an error}
    #' \item{`config`}{miscellaneous configuration settings such as the file name}
    #' }
    result_list = function(n = Inf, trt_tf = identity, out_tf = identity) {
      done <- private$.done()
      n <- min(n, length(done))
      i <- seq(1, length(done), length.out = n) |> round()
      map(done[i],
          if(identical(trt_tf, identity) && identical(out_tf, identity))
            function(fn) c(safe_readRDS(fn, verbose = TRUE), rds = fn)
          else
            function(fn) {
              o <- safe_readRDS(fn, verbose = TRUE)
              o$treatment <- trt_tf(o$treatment)
              if (o$OK) o$output <- out_tf(o$output)
              c(o, rds = fn)
            },
          .progress = "Loading results")
    },

    #' @description Scan through the results files and collate them into a data frame.
    #' @param trt_tf,out_tf functions that take the treatment configuration list and the output respectively, and return named lists that become data frame columns; a special value `I` instead creates columns `treatment` and/or `output` with the respective lists copied as is.
    #' @param rds whether to include an `.rds` column described below.
    #' @param ... additional arguments, passed to `Piecemeal$result_list()`.
    #' @return A data frame with columns corresponding to the values returned by `trt_tf` and `out_tf`, with the following additional columns:
    #' \describe{
    #' \item{`.seed`}{the random seed used.}
    #' \item{`.rds`}{the path to the RDS file (if requested).}
    #' }
    #' Runs that erred are filtered out.
    result_df = function(trt_tf = identity, out_tf = identity, rds = FALSE, ...) {
      if(identical(trt_tf, I)) trt_tf <- \(x) list(treatment=list(x))
      if(identical(out_tf, I)) out_tf <- \(x) list(output=list(x))

      l <- self$result_list(trt_tf = trt_tf, out_tf = out_tf, ...)
      OK <- map_lgl(l, "OK")
      if(!all(OK)) message(sprintf("%d/%d runs had returned an error.", sum(!OK), length(OK)))
      l <- l[OK]

      map(l, function(o) {
        structure(c(list(),
                    o$treatment, o$output,
                    list(.seed = o$seed), if(rds) list(.rds = o$rds)),
                  class = "data.frame", row.names = 1L)
      }, .progress = "Converting columns") |>
        do.call(rbind, args = _)
    },

    #' @description Clear the simulation results so far.
    #' @param confirm whether the user should be prompted to confirm deletion.
    reset = function(confirm = interactive()) {
      done <- length(private$.done())
      if(done && confirm){
        ans <- readline(paste0('This will delete ', done, ' result files and any lock files from ', sQuote(private$.outdir), '. Are you sure? ("yes" to confirm, anything else to cancel) '))
        if(ans != "yes"){
          message("Cancelled.")
          invisible(self)
        }
      }
      unlink(private$.outdir, recursive = TRUE)
      invisible(self)
    },

    #' @description Delete the result files for which the worker function failed and/or for which the files were corrupted, or based on some other predicate.
    #' @param which a function of a result list (see `Piecemeal$result_list()`) returning `TRUE` if the result file is to be deleted and `FALSE` otherwise.
    clean = function(which = function(res) !res$OK) {
      done <- private$.done()
      del <- done |> map_lgl(\(fn) which(safe_readRDS(fn)), .progress = "Loading and filtering")
      private$.toclean <- FALSE
      if(any(del)){
        message("Deleting...")
        unlink(done[del])
        message(sprintf("%d failed runs deleted.", sum(del)))
      }
      invisible(self)
    },

    #' @description List the configurations for which the worker function failed.
    erred = function() {
      private$.done() |>
        map(function(fn) {
          o <- safe_readRDS(fn)
          if(o$OK) NULL else o
        }) |>
        compact()
    },

    #' @description Set miscellaneous options.
    #' @param split a two-element vector indicating whether the output files should be split up into subdirectories and how deeply, the first for splitting configurations and the second for splitting seeds; this can improve performance on some file systems.
    #' @param error how to handle worker errors:\describe{
    #' \item{`"save"`}{save the seed, the configuration, and the status, preventing future runs until the file is using `Piecemeal$clean()`.}
    #' \item{`"skip"`}{return the error message as a part of `run()`'s return value, but do not save the RDS file; the next `run()` will attempt to run the worker for that configuration and seed again.}
    #' \item{`"stop"`}{allow the error to propagate; can be used in conjunction with `Piecemeal$cluster(NULL)` and (global) `options(error = recover)` to debug the worker.}
    #' \item{`"auto"`}{(default) as `"save"`, but if any of the methods that change how each configuration is run (i.e., `$worker()`, `$setup()`, and `$export_vars()`) is called, `$clean()` will be called automatically before the next `$run()`.}
    #' }
    options = function(split = c(1L, 1L), error = c("auto", "save", "skip", "stop")) {
      if(!missing(split)) private$.split <- rep_len(split, 2L)
      if(!missing(error)) private$.error <- match.arg(error)
      invisible(self)
    },

    #' @description Print the current simulation settings, including whether there is enough information to run it.
    #' @param ... additional arguments, currently unused.
    print = function(...) {
      cat("A Piecemeal simulation\n")
      cat("Output directory:", private$.outdir, "\n")
      cat("\nDesign:", max(1, length(private$.treatments)), "treatment configurations by", length(private$.seeds), "seeds =", max(1, length(private$.treatments)) * length(private$.seeds), "runs\n")

      if(is.null(private$.cl_setup)) cat("\nNo cluster set up.\n")
      else{
        cat("\nCluster:\n")
        if(inherits(private$.cl_setup, "cluster")) {
          cat("  preexisting: ")
          print(private$.cl_setup)
        } else {
          cat("  configuration: ")
          print(as.call(c(list(as.name("makeCluster")), private$.cl_setup)))
        }
      }

      setup <- length(private$.setup) && !identical(private$.setup, quote({}))
      vars <- (sum(length(private$.cl_vars)) != 0)
      if(setup || vars) cat("\nSetup:\n")

      if(setup) {
        cat("  initialise each node with:\n")
        cat(paste("   ", deparse(private$.setup), collapse = "\n"), "\n")
      }

      if(vars) {
        cat("  variables to copy from each environment:\n")
        for(i in seq_along(private$.cl_vars)) {
          cat("    ", envname(private$.cl_var_envs[[i]]), ": ",
              paste(sQuote(private$.cl_vars[[i]]), collapse = ", "),
              "\n", sep = "")
        }
      }

      if(length(private$.worker)) {
        cat("\nCall for each configuration and seed:\n")
        cat(paste(" ", deparse(private$.worker), collapse = "\n"), "\n")
      }

      cat("\nOptions:\n")
      cat("  directory split:", private$.split[1], "level(s) by treatment and", private$.split[2], "level(s) by seed\n")
      cat("  errored runs:", private$.error, "\n\n")

      cat("Ready to execute?", if(!inherits(try(private$.check_args()), "try-error") && length(private$.seeds) && length(private$.worker)) "Yes." else "No.", "\n")
    },

    #' @description Summarise the current state of the simulation, including the number of runs succeeded, the number of runs still to be done, the errors encountered, and, if started, the estimated time to completion at the current rate.
    #' @param ... additional arguments, currently passed to `Piecemeal$eta()`.
    status = function(...) {
      # We only want the output if it's an error.
      l <- self$result_list(trt_tf = \(trt) NULL, out_tf = \(out) if(is(out, "try-error")) out else NULL)
      Result <- map_chr(l, function(o)
        if(o$OK) "Done"
        else if(is.null(o$config)) "Corrupted"
        else trimws(o$output) # the error message
        )

      total <- max(1, length(private$.treatments)) * length(private$.seeds)
      inprog <- length(private$.doing())
      Result <- c(Result,
                  rep_len("Running (approx.)", inprog),
                  rep_len("ToDo", max(0, total - length(l) - inprog)))

      o <- table(Result)
      attr(o, "outdir") <- private$.outdir
      if ("Done" %in% o) attr(o, "eta") <- self$eta(...)
      class(o) <- c("Piecemeal_status", class(o))
      o
    },

    #' @description Estimate the rate at which runs are being completed and the estimated time left.
    #' @param window initial time window to use, either a [`difftime`] object or the number in seconds; defaults to 1 hour.
    #' @details The window used is actually between the last completed run and the earliest run in the `window` before that. This allows to take an interrupted simulation and estimate how much more time (at the most recent rate) is needed.
    #' @note The estimation method is a simple ratio, so it may be biased under some circumstances. Also, it does not check if the runs have been completed successfully.
    #' @return A list with elements `window`, `recent`, `cost`, `left`, `rate`, and `eta`, containing, respectively, the time window, the number of runs completed in this time, the average time per completion, the estimated time left (all in seconds), the corresponding rate (in Hertz), and the expected time of completion.
    eta = function(window = 3600) {
      if(is(window, "difftime")) window <- as.numeric(window, units = "secs")

      done <- private$.done()
      inprog <- length(private$.doing()) # Not used in the calculation.

      total <- max(1, length(private$.treatments)) * length(private$.seeds)
      left <- total - length(done)

      mtimes <- done |>
        file.info(extra_cols = FALSE) |>
        pluck("mtime")
      mtimes <- mtimes[mtimes >= max(mtimes) - window]

      recent <- length(mtimes) - 1
      window <- as.numeric(max(mtimes) - min(mtimes), units = "secs")

      cost <- window / recent
      structure(list(window = window, recent = recent, cost = cost, rate = 1/cost, left = left * cost, eta = Sys.time() + left * cost, inprog = inprog),
                class = "Piecemeal_eta", outdir = private$.outdir)
    }
  )
  )

#' @noRd
#' @export
summary.Piecemeal <- function(object, ...) {
  object$status(...)
}

#' @noRd
#' @export
print.Piecemeal_status <- function(x, ...) {
  cat("A Piecemeal simulation\n")
  cat("Output directory:", attr(x, "outdir"), "\n\n")
  print(as.data.frame(x))
  if (!is.null(attr(x, "eta"))) {
    cat("\n")
    print(attr(x, "eta"))
  }
  invisible(x)
}

# NB: toString() and as.character() err, deparse() only returns
# "<environment>", and environmentName() only works for namespaces.
envname <- function(e) {
  capture.output(e) |>
    gsub("^<environment: (.+)>$", "\\1", x = _)
}

find_time_unit <- function(dt) {
  dt <- as.numeric(dt, units = "secs")
  units <- if(dt < 60*2) "secs"
           else if(dt < 60*60*2) "mins"
           else "hours"

  as.difftime(dt / switch(units, secs = 1, mins = 60, hours = 60*60), units = units)
}

find_rate_unit <- function(hz) {
  per <- if(hz >= 1) "sec"
         else if(hz >= 1/60) "min"
         else if(hz >= 1/60/60) "hour"
         else "day"

  structure(hz * switch(per, sec = 1, min = 60, hour = 60*60, day = 24*60*60), per = per, class = "rate")
}

format.rate <- function(x, ...) {
  paste(format(as.numeric(x), ...), "per", attr(x, "per"))
}

#' @noRd
#' @export
print.Piecemeal_eta <- function(x, ...) {
  cat("A Piecemeal simulation ETA calculation\n")
  cat("Output directory:", attr(x, "outdir"), "\n")
  cat("Based on", x$recent, "completions in", format(find_time_unit(x$window), digits = 1), "\n\n")

  cat("Time per completion:", format(find_time_unit(x$cost), digits = 1), "\n")
  cat("Completion rate:", format(find_rate_unit(x$rate), digits = 1), "\n")
  cat("Estimated time left:", format(find_time_unit(x$left), digits = 1), "\n")
  cat("Estimated completion time:", format(x$eta), "\n\n")

  cat("In progress (approximate):", x$inprog, "\n")

  invisible(x)
}

add_hash <- function(x) {
  attr(x, "hash") <- NULL
  structure(x, hash = rlang::hash(x))
}

config_fn <- function(config) {
  paste(attr(config$treatment, "hash"), config$seed, "rds", sep = ".")
}

by2char <- function(s, n, where = c("start", "end")) {
  start <- switch(match.arg(where),
                  start = 0L,
                  end = nchar(s) - n * 2L)
  map_chr(seq_len(n), \(i) substr(s, start + i*2L - 1L, start + i*2L))
}

config_subdir <- function(config, split = c(0L, 0L)) {
  hash <- attr(config$treatment, "hash")
  seed <- as.character(config$seed)
  subdirs <- if(split[1L]) by2char(hash, split[1L], "start")
  c(subdirs, if(split[2L]) by2char(seed, split[2L], "end"))
}

## This should really be done by an appropriate system call, perhaps
## via ticket https://github.com/r-lib/filelock/issues/23 . It is
## theoretically possible for another process to attempt to lock path
## between the lock() call and the unlink() call and be rebuffed.
##
## NB: This function also cleans stale lockfiles as a side-effect.
is_locked <- function(path) {
  # No lock file -> not locked.
  if (!file.exists(path)) return(FALSE)

  lock <- filelock::lock(path, timeout = 0)

  # Lock attempt unsuccessful -> locked
  if (is.null(lock)) return(TRUE)

  # Delete and unlock -> unlocked
  unlink(path)
  filelock::unlock(lock)
  FALSE
}

safe_readRDS <- function(file, ..., verbose = FALSE) {
  tryCatch(readRDS(file, ...),
           error = function(e) {
             if(verbose) message("Run file ", sQuote(file), " is corrupted. This should never happen.")
             list(seed = NULL, treatment = NULL, output = NULL, config = NULL, OK = FALSE)
           })
}

run_config <- function(config, error, env = NULL) {
  worker <- get(".worker", env %||% .GlobalEnv)
  outdir <- get(".outdir", env %||% .GlobalEnv)

  fn <- config$fn
  subdirs <- config$subdirs
  dn <- do.call(file.path, c(list(outdir), subdirs))
  dir.create(dn, recursive = TRUE, showWarnings = FALSE)
  fn <- file.path(dn, fn)

  if(file.exists(fn)) return(paste(fn, "SKIPPED", sep = "\n")) # If this treatment + seed combination has been run, move on.

  # Or, if it's already being run by another process, move on; otherwise, lock it.
  fnlock <- filelock::lock(paste0(fn, ".lock"), timeout = 0)
  on.exit({
    filelock::unlock(fnlock)
    unlink(paste0(fn, ".lock"))
  })
  if(is.null(fnlock)) return(paste(fn, "SKIPPED", sep = "\n"))

  treatment <- config$treatment
  config$treatment <- NULL
  seed <- config$seed
  config$seed <- NULL

  if(".seed" %in% names(formals(worker)))
    treatment$.seed <- seed

  set.seed(seed)
  out <- if (error == "stop") do.call(worker, treatment, envir = env %||% .GlobalEnv)
         else try(do.call(worker, treatment, envir = env %||% .GlobalEnv), silent = TRUE)
  if(inherits(out, "try-error")) {
    if(error == "skip") return(paste(fn, out, sep = "\n"))
    OK <- FALSE
  } else OK <- TRUE
  

  # saveRDS() is not atomic, whereas file.rename() typically is. The
  # following pattern guarantees that if the process is killed while
  # the results are being written out, a corrupted file does not
  # result.
  saveRDS(list(seed = seed, treatment = treatment, output = out, config = config, OK = OK), paste0(fn, ".tmp"))
  file.rename(paste0(fn, ".tmp"), fn)
  if(OK) paste(fn, "OK", sep = "\n") else paste(fn, out, sep = "\n")
}
