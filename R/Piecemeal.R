# These names are not syntactic R names so will hopefully not clash
# with anything the user might do. They also produce prettier error
# messages.
#
# TODO: Make these user-customisable?
worker_var <- "<worker>"
outdir_var <- "<output directory>"
reserved_vars <- c(worker_var, outdir_var)

#' The `Piecemeal` [`R6`] Class
#'
#' @description This class exports methods for configuring a simulation, running it, debugging failed configurations, and resuming the simulation. See [the vignette `vignette("piecemeal")`](../doc/piecemeal.html) for a long worked example. Examples of \R scripts suitable for non-interactive use on a computing cluster can be found in your package installation's \file{examples} directory, which can be located by running `system.file("examples", package = "piecemeal")`. (This appears to currently be \file{\Sexpr[stage=render]{system.file("examples", package = "piecemeal")}}.)
#'
#' @details A chain of `R6` method calls is used to specify the setup and the worker functions, the treatment configurations to be passed to the worker, and parallelism and other simulation settings. Then, when `$run()` is called, the cluster is started (if not already running), worker nodes are initialised, and every combination of random seed and treatment configuration is passed to [clusterApplyLB()] (if parallel processing is enabled).
#'
#' On the worker nodes, the worker function is not called directly; rather, care is taken to make sure that the specified configuration and seed is not already being worked on. This makes it safe to, e.g., queue multiple jobs for the same simulation. If the configuration is available, `set.seed()` is called with the seed and then the worker function is run.
#'
#' Errors in the worker function are caught and error messages saved and returned.
#'
#' When running with parallel processing disabled, `Piecemeal` tries to mimic the environment of a cluster as closely as possible while still allowing interactive debugging via `$test()` and `$debug()`. Specifically:
#'
#' * The worker function's own environment is ignored. That is, if the worker function is defined in an environment that contains a certain variable, the worker function will not be able to "see" it unless it was explicitly exported.
#' * The random number generator state ([`.Random.seed`]) is saved before the configuration's seed is set and run and restored after.
#' * However, the [search()] path (packages attached by [library()]) is global to an \R session, so the mimicry is imperfect: any packages attached on the manager session will be visible to the worker function run locally but not to one run on a cluster node, unless a part of `$setup()`.
#' 
#' @param error sets the [options()] `error=` option before calling the worker; special values `"debug"` or `"debugonce"` (with or without quotes) will instead debug the worker function from the start, i.e., as if [debugonce()] were called on it first. See the [the vignette `vignette("piecemeal")`](../doc/piecemeal.html) for an illustration.
#'
#' @examples
#' # Initialise, with the output directory.
#' sim <- piecemeal::init(file.path(tempdir(), "piecemeal_demo"))
#' # Clear the previous simulation, if present.
#' sim$reset()
#'
#' # Set up a simulation:
#' sim$
#'   # for every combination of x = 1, 2 and y = 1, 3, 9, 27,
#'   factorial(x = 2^(0:1), y = 3^(0:3))$
#'   # each replicated 3 times,
#'   nrep(3)$
#'   # first load library 'rlang', once per node,
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
#' @importFrom cli cli_progress_along cli_progress_message cli_progress_done cli_inform cli_alert_info
#' @importFrom cli cli_alert_success cli_alert_warning cli_abort cli_rule cli_text cli_warn
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
    .toclean = TRUE,
    .treatment_by_hash = function(hash) {
      detect(private$.treatments, function(x) attr(x, "hash") == hash)
    },
    .config_by_hash_seed = function(hash, seed) {
      list(treatment = .treatment_by_hash(hash), seed = as.integer(seed))
    },
    .setup_env = function(cl = NULL) {
      if(is.null(cl)) {
        # Set up an environment for the initial setup evaluation.
        my_global_env <- new.env(parent = parent.env(.GlobalEnv))
        my_global_env[[worker_var]] <- private$.worker
        my_global_env[[outdir_var]] <- private$.outdir

        eval(private$.setup, envir = my_global_env)

        # Recreate the environment, since setup code might have added
        # more libraries to the search path.
        my_global_env <- rlang::env_clone(my_global_env, parent = parent.env(.GlobalEnv))
        environment(my_global_env[[worker_var]]) <- my_global_env

        for(i in seq_along(private$.cl_vars))
          for(name in private$.cl_vars[[i]])
            assign(name, get(name, private$.cl_var_envs[[i]]), my_global_env)

        my_global_env
      } else {
        assign(worker_var, private$.worker)
        assign(outdir_var, private$.outdir)
        clusterExport(cl, c(worker_var, outdir_var), environment())

        clusterCall(cl, eval, private$.setup, envir = .GlobalEnv)

        for(i in seq_along(private$.cl_vars))
          clusterExport(cl, private$.cl_vars[[i]], private$.cl_var_envs[[i]])
        NULL
      }
    },
    .done = function() {
      # Get individual .rds files
      cli_progress_message("Finding individual runs")
      files <- list.files(private$.outdir, ".*\\.rds$", full.names = TRUE, recursive = TRUE)
      cli_progress_done()

      # Get files from consolidated database
      con <- db_connect(private$.outdir)
      if (!is.null(con)) {
        cli_progress_message("Finding consolidated runs")
        # Interrupted consolidation may result in an rds file inserted
        # into the database but not deleted, so make sure it's only
        # listed once.
        db_files <- db_list_filenames(con) |> setdiff(basename(files))
        # Return full paths for consistency (use a virtual path prefix)
        db_files <- file.path(private$.outdir, ".consolidated", db_files)
        files <- c(files, db_files)
        cli_progress_done()
      }

      files
    },
    .doing = function() {
      cli_progress_message("Finding running workers")
      files <- list.files(private$.outdir, ".*\\.rds.lock$",
                          full.names = TRUE, recursive = TRUE)
      cli_progress_done()
      ## This is done because purrr::keep() doesn't (officially)
      ## support .progress= arguments. See
      ## https://github.com/tidyverse/purrr/issues/1249 .
      ##
      ## TODO: Check that this is still the case before each release.
      files[map_lgl(files, is_locked, .progress = "Confirming running workers")]
    },
    .check_args = function(which = TRUE) {
      anames <- suppressWarnings(names(formals(private$.worker)))
      if (length(private$.treatments)) {
        for(i in seq_along(private$.treatments)[which]) {
          treatment <- private$.treatments[[i]]
          if(".seed" %in% names(treatment))
            cli_abort(c(
              "x" = "In treatment configuration {i}, argument {.arg .seed} is reserved by {.pkg piecemeal}.",
              "i" = "To provide your own seed, use a different argument name."
            ))
          astart <- if(".seed" %in% anames) list("", ".seed") else list("")

          tryCatch(match.call(private$.worker, as.call(c(astart, treatment))),
                   error = function(e){
                     e$message <- paste0(e$message, " in treatment configuration ", i)
                     e$call <- NULL
                     stop(e)
                   })
        }
      } else {
        astart <- if(".seed" %in% anames) list("", ".seed") else list("")

        tryCatch(match.call(private$.worker, as.call(astart)),
                 error = function(e){
                   e$message <- paste0(e$message, " in treatment configuration ", i)
                   e$call <- NULL
                   stop(e)
                 })
      }
    },
    .eta = function(window = 3600, done, doing) {
      if(is(window, "difftime")) window <- as.numeric(window, units = "secs")

      inprog <- length(doing) # Not used in the calculation.

      total <- max(1, length(private$.treatments)) * length(private$.seeds)
      left <- total - length(done)

      mtimes <- get_file_mtimes(private$.outdir, done)
      mtimes <- mtimes[mtimes >= suppressWarnings(max(mtimes)) - window]

      recent <- length(mtimes) - 1
      window <- as.numeric(suppressWarnings(max(mtimes) - min(mtimes)), units = "secs")

      cost <- window / recent
      structure(list(window = window, recent = recent, cost = cost, rate = 1/cost, left = left * cost, eta = Sys.time() + left * cost, inprog = inprog),
                class = "Piecemeal_eta", outdir = private$.outdir)
    }
  ),

  public = list(

    #' @description Create a new `Piecemeal` instance.
    #' @param outdir the directory to hold the partial simulation results.
    initialize = function(outdir) {
      private$.outdir <- gsub("/$", "", outdir)
    },

    #' @description Cluster settings for the piecemeal run.
    #' @param ... either arguments to [makeCluster()] or a single argument containing either an existing cluster or `NULL` to disable parallel computing.
    cluster = function(...) {
      spec <- list(...)
      if(length(spec) == 1L && (is.null(spec[[1]]) || is(spec[[1]], "cluster"))) private$.cl_setup <- spec[[1]]
      else private$.cl_setup <-
             if (hasName(spec, "setup_timeout")) spec
             else c(spec, list(setup_timeout = 3600))
      invisible(self)
    },

    #' @description Specify variables to be copied from the manager node to the worker nodes' global environment. (See [parallel::clusterExport()].)
    #' @param varlist a character vector with variable names.
    #' @param envir the environment on the manager node from which to take the variables; defaults to the current environment.
    #' @param .add whether the new variables should be added to the current list (if `TRUE`, the default) or replace it (if `FALSE`).
    export_vars = function(varlist, envir = parent.frame(), .add = TRUE) {
      if (any(varlist %in% reserved_vars))
        cli_abort("Variable names {.var {reserved_vars}} are reserved for internal use.")
      private$.toclean <- TRUE
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
      private$.toclean <- TRUE
      private$.setup <- substitute(expr)
      invisible(self)
    },

    #' @description Specify the function to be run for each treatment configuration; it will be run in the global environment.
    #' @param fun a function whose arguments are specified by `$treatments()` and `$factorial()`; if it has `.seed` as a named argument, the seed will be passed as well.
    #' @details If no treatment is specified, the function is called with no arguments (or just `.seed`).

    worker = function(fun) {
      private$.toclean <- TRUE
      private$.worker <- fun
      invisible(self)
    },

    #' @description Specify a list of treatment configurations to be run.
    #' @param l a list, typically of lists of arguments to be passed to the function specified by `worker`; it is recommended that these be as compact as possible, since they are [`serialize`]d and sent to the worker node for every combination of treatment configuration and random seed.
    #' @param .add whether the new treatment configurations should be added to the current list (if `TRUE`, the default) or replace it (if `FALSE`).
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

    #' @description Test-run some treatment configurations on the local system and return their results without saving.
    #' @details This function ignores cluster settings and bypasses the usual bookkeeping: the given configuration is run even if the result file already exists, and the results are returned and not saved. If the run results in an error, it is handled according to the `error` argument.
    #' @param config,shuffle which configurations to run; the configurations can be specified as follows:
    #' * `config` a [`list`] of treatment configurations in the same format as that of `Piecemeal$todo()`. If only passing one configuration, remember to wrap it in [list()].
    #' * `config` a character vector of the form `c(treatment_hash, seed)` (or multiple such concatenated). (The seed will be converted back to an integer.)
    #' * `config` a number and `shuffle == TRUE` (the default): run `config` configurations, chosen at random from those left to do.
    #' * `config` a numeric vector and `shuffle == FALSE`: `config` is treated as a vector of indices from the list returned by `Piecemeal$todo()`.
    #' @return A list containing the results of the runs, with each sublist's element `$output` containing the value returned by the worker. They are not saved.
    test = function(config = 1, shuffle = TRUE, error = getOption("error")) {
      private$.check_args()
      run_env <- private$.setup_env()
      configs <- if (is.list(config) && every(config, function(x) is.list(x) && all(hasNames(x, c("seed", "treatment"))))) {
                   config
                 } else if (is.numeric(config)) {
                   if (shuffle) sample(self$todo(), config)
                   else self$todo()[config]
                 } else if (is.character(config) && length(config) %% 2L == 0L) {
                   hashes <- config[seq_along(config) %% 2L == 1]
                   seeds <- config[seq_along(config) %% 2L == 0]

                   map2(hashes, seeds, private$.config_by_hash_seed)
                 } else {
                   cli_abort("Invalid configuration specification.")
                 }

      map(configs, function(config) {
        cli_rule(
          left = "Running configuration {.val {attr(config$treatment, 'hash')}}",
          right = "seed {.val {config$seed}}"
        )

        if (some(list(debug, debugonce, "debug", "debugonce"), identical, error)) {
          debugonce(run_env[[worker_var]])
        } else {
          o <- options(error = error)
          on.exit(options(o))
        }

        run_config(config, error = ".debug", env = run_env)
      })
    },

    #' @description Run the simulation.
    #' @param shuffle Should the treatment configurations be run in a random order (`TRUE`, the default) or in the order in which they were added (`FALSE`)?
    #' @return Invisibly, a character vector with an element for each seed and treatment configuration combination attempted, indicating its file name and status, including errors.
    run = function(shuffle = TRUE) {
      if(private$.error == "auto" && private$.toclean) self$clean()
      private$.check_args()

      cl <- private$.cl_setup
      if(!is.null(cl) && !is(cl, "cluster")) {
        cl <- do.call(makeCluster, cl)
        on.exit(stopCluster(cl))
      }

      run_env <- private$.setup_env(cl) # run_env returned only when running locally.

      configs <- self$todo()
      done <- attr(configs, "done")
      cli_alert_info("Starting {length(configs)} runs ({done} already done).")

      if(shuffle) configs <- configs[sample.int(length(configs))]

      statuses <- simplify2array(
        if(is.null(cl)) map(configs, run_config, error = private$.error, env = run_env, .progress = "Running")
        else clusterApplyLB(cl, configs, run_config, error = private$.error)
      )

      status_counts <- c(
        if(length(statuses)) statuses |> strsplit("\n", fixed = TRUE) |> map_chr(2L),
        rep("SKIPPED", done)
      ) |>
        table()
      summary <- if(length(status_counts))
        paste(names(status_counts), as.integer(status_counts),
              sep = ": ") |> setNames(rep(" ", length(status_counts)))
      else "No runs."
      cli_inform(c("Run summary:", summary))

      invisible(if(length(statuses)) statuses else character(0))
    },

    #' @description Run the simulation if in a non-interactive session and at top level (that is, not [source()]d); otherwise print a message and do nothing.
    #' @param ... arguments passed to `Piecemeal$run()`.
    #' @param call_depth how many call frames deep is the `autorun()` call allowed to be? Set to `+Inf` or a large number to disable the check.
    #' @details This method can be used in place of `Piecemeal$run()` to allow the same \file{.R} file to be run in a batch job to run the simulation or in an interactive session or from another script to facilitate monitoring, debugging, consolidation, and exporting results. By default, its behaviour is based on [base::interactive()] and [base::sys.nframe()], but it can be overridden by setting `options(piecemeal.autorun = TRUE/FALSE)` to force it to always run (if `TRUE`) never run (if `FALSE`). Setting to `NA` or unsetting reverts to the default behaviour. See the package installation's \file{examples} directory for usage examples.
    autorun = function(..., call_depth = 0L) {
      piecemeal.autorun <- getOption("piecemeal.autorun", NA)

      if (is.na(piecemeal.autorun %||% NA)) {
        if (!interactive() &&
            sys.nframe() <= call_depth + 1L) # +1 for $autorun() itself.
          self$run(...)
        else
          cli_inform(c("!" = "Interactive session or indirect call detected: {.code Piecemeal$autorun()} not starting.",
                       "i" = "To start unconditionally, use {.code Piecemeal$run()} or {.code options(piecemeal.autorun = TRUE)}."))
      } else if(piecemeal.autorun) {
        cli_inform(c("i" = "{.code Piecemeal$autorun()} forced by {.code options(piecemeal.autorun = TRUE)}."))
        self$run(...)
      } else cli_inform(c("i" = "{.code Piecemeal$autorun()} suppressed by {.code options(piecemeal.autorun = FALSE)}."))
    },

    #' @description List the configurations still to be run.
    #' @return A list of lists with arguments to the worker functions and worker-specific configuration settings (particularly with elements `$seed` with the random seed and `$treatment` with the arguments to the worker); also an attribute `"done"` giving the number of configurations skipped because they are already done.
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
    #' \item{`output`}{value returned by the worker, or a `try-error` returned by [try()]}
    #' \item{`OK`}{whether the worker succeeded or produced an error}
    #' \item{`config`}{miscellaneous configuration settings such as the file name}
    #' }
    result_list = function(n = Inf, trt_tf = identity, out_tf = identity) {
      con <- db_connect(private$.outdir)
      done <- private$.done()
      n <- min(n, length(done))
      i <- seq(1, length(done), length.out = n) |> round()
      map(done[i],
          if(identical(trt_tf, identity) && identical(out_tf, identity))
            function(fn) c(read_result(private$.outdir, fn, con), rds = fn)
          else
            function(fn) {
              o <- read_result(private$.outdir, fn, con)
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
      if(!all(OK))
        cli_alert_warning("{sum(!OK)}/{length(OK)} runs returned an error.")
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
        cli_alert_warning(
          "This will delete {done} result files and any lock files from {.path {private$.outdir}}."
        )
        cli_text("Are you sure? Type {.val yes} to confirm; anything else cancels.")
        ans <- readline("Confirmation: ")
        if(ans != "yes"){
          cli_alert_info("Cancelled.")
          invisible(self)
        }
      }
      unlink(private$.outdir, recursive = TRUE)
      invisible(self)
    },

    #' @description Delete the result files for which the worker function produced an error and/or which were somehow corrupted, or based on some other predicate.
    #' @param which a function of a result list (see `Piecemeal$result_list()`) returning `TRUE` if the result file is to be deleted and `FALSE` otherwise.
    #' @details If `Piecemeal$options(error = "auto")` (the default) is set, reinitialising the `Piecemeal` object or changing some configuration settings, including the worker function, the setup code, and the exported variables, will automatically set a flag to run `clean()` at the start of the next run.
    clean = function(which = function(res) !res$OK) {
      done <- private$.done()
      del <- done |> map_lgl(\(fn) which(safe_readRDS(fn)), .progress = "Loading and filtering")
      private$.toclean <- FALSE
      if(any(del)){
        cli_alert_info("Deleting...")
        unlink(done[del])
        cli_alert_success("{sum(del)} failed runs deleted.")
      }
      invisible(self)
    },

    #' @description List the configurations for which the worker function failed.
    #' @param n return up to this many errors.
    erred = function(n = Inf) {
      con <- db_connect(private$.outdir)

      nerr <- 0L
      private$.done() |>
        map(function(fn) {
          if (nerr >= n) return(NULL)
          o <- read_result(private$.outdir, fn, con)
          if(o$OK) NULL
          else {
            nerr <<- nerr + 1
            o
          }
        }) |>
        compact()
    },

    #' @description Debug the worker for a particular configuration.
    #' @inherit Piecemeal$test details
    #' @param result either a list in the `Piecemeal$todo()` format or a number indexing the list returned by `Piecemeal$erred()`.
    #' @return The result list, with element `$output` containing the value returned by the worker.
    debug = function(result = 1, error = recover) {
      private$.check_args()
      result <- if (is.list(result) && all(hasNames(result, c("seed", "treatment")))) result
                else if (is.numeric(result)) result <- self$erred(n = result)[[result]]
                else cli_abort("Invalid result specification.")

      run_env <- private$.setup_env()
      if (some(list(debug, debugonce, "debug", "debugonce"), identical, error)) {
        debugonce(run_env[[worker_var]])
      } else {
        o <- options(error = error)
        on.exit(options(o))
      }
      run_config(result, error = ".debug", env = run_env)
    },

    #' @description Consolidate successful run result files into a SQLite database.
    #' @details This method consolidates individual RDS result files into a single database to reduce inode usage. Only successful runs (where `OK == TRUE`) are consolidated. This function is safe to run while simulations are running and to interrupt (using \kbd{CTRL-C} or analogous) and resume, but only one consolidation may be run at the same time. Consolidated and unconsolidated results can be accessed transparently.
    #' @return Invisibly, the number of files consolidated.
    consolidate = function() {
      count <- consolidate_results(private$.outdir)
      if (count > 0) {
        cli_alert_success("{count} files consolidated.")
      }
      invisible(count)
    },

    #' @description Set miscellaneous options.
    #' @param split a two-element vector indicating whether the output files should be split up into subdirectories and how deeply, the first for splitting by configuration and the second for splitting by seed; this can improve performance on some file systems.
    #' @param error how to handle worker errors:\describe{
    #' \item{`"save"`}{save the seed, the configuration, and the status, preventing future runs until the file is removed using `Piecemeal$clean()`.}
    #' \item{`"skip"`}{return the error message as a part of `Piecemeal$run()`'s return value, but do not save the RDS file; the next `Piecemeal$run()` will attempt to run the worker for that configuration and seed again.}
    #' \item{`"stop"`}{allow the error to propagate; can be used in conjunction with `Piecemeal$cluster(NULL)` and (global) `options(error = recover)` to debug the worker, though `Piecemeal$debug()` method is probably more convenient.}
    #' \item{`"auto"`}{(default) as `"save"`, but if the `Piecemeal` is reinitialised or any of the methods that change how each configuration is run (i.e., `Piecemeal$worker()`, `Piecemeal$setup()`, and `Piecemeal$export_vars()`) is called, `Piecemeal$clean()` will be called automatically at the start of the next `Piecemeal$run()`.}
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

    #' @description Summarise the current status of the simulation, including the number of runs succeeded, the number of runs still to be done, the number of runs currently running, the errors encountered, and, if started, the estimated time to completion at the current rate.
    #' @param ... additional arguments, currently passed to `Piecemeal$eta()`.
    #' @return An object of class `Piecemeal_status` containing a frequency [`table`] summarising simulation outcomes including frequencies of different error messages, attribute `"eta"` with a `Piecemeal_eta` as well as a number of other attributes useful for printing the simulation status.
    status = function(...) {
      # Get all done files
      done <- private$.done()
      
      # Separate consolidated from individual files
      # Consolidated files are in the database and have .consolidated directory in their path
      # Use .Platform$file.sep to handle both Unix (/) and Windows (\) separators
      consolidated_pattern <- paste0(.Platform$file.sep, ".consolidated", .Platform$file.sep)
      is_consolidated <- grepl(consolidated_pattern, done, fixed = TRUE)
      
      # For consolidated files, we know they're successful (only successful runs get consolidated)
      n_consolidated <- sum(is_consolidated)
      
      # For individual files, we need to check their status
      individual_files <- done[!is_consolidated]
      Result <- character(0)
      
      if (length(individual_files) > 0) {
        # Read only individual files to check their status
        Result <- map_chr(individual_files, function(fn) {
          o <- safe_readRDS(fn)
          if(o$OK) "Done"
          else if(is.null(o$fn)) "Corrupted"
          else trimws(o$output) # the error message
        }, .progress = "Checking individual results")
      }
      
      # Add all successful consolidated runs as "Done"
      Result <- c(Result, rep_len("Done & consolidated", n_consolidated))

      total <- max(1, length(private$.treatments)) * length(private$.seeds)
      doing <- private$.doing()
      inprog <- length(doing)
      Result <- c(Result,
                  rep_len("Running (approx.)", inprog),
                  rep_len("ToDo", max(0, total - length(done) - inprog)))

      o <- table(Result)     
      if (sum(o[startsWith(names(o), "Done")]) > 1L)
        attr(o, "eta") <- private$.eta(..., done = done, doing = doing)
      # NB: last_*() should be called after .eta(), because the latter
      # takes a nontrivial amount of time.
      structure(o,
                outdir = private$.outdir,
                last_OK = self$last_OK(),
                last_consolidated = self$last_consolidated(),
                class = c("Piecemeal_status", class(o)))
    },

    #' @description Estimate the rate at which runs are being completed and how much more time is needed.
    #' @param window initial time window to use, either a [`difftime`] object or the number in seconds; defaults to 1 hour.
    #' @details The window used is actually between the last completed run and the earliest run in the `window` before that. This allows to take an interrupted simulation and estimate how much more time (at the most recent rate) is needed.
    #'
    #' The estimation method is a simple ratio, so it may be biased under some circumstances. Also, it does not check if the runs have been completed successfully.
    #' @return An object of class `Piecemeal_eta` containing a list with elements `window`, `recent`, `cost`, `left`, `rate`, and `eta`, containing, respectively, the time window, the number of runs completed in this time, the average time per completion, the estimated time left (all in seconds), the corresponding rate (in Hertz), and the expected time of completion.
    eta = function(window = 3600) {
      private$.eta(window, private$.done(), private$.doing())
    },

    #' @description Return the last time a run has finished successfully.
    last_OK = function() {
      file.info(file.path(private$.outdir, "last_OK"), extra_cols = FALSE)$mtime
    },

    #' @description Return the last time the consolidated database had been updated.
    last_consolidated = function() {
      file.info(file.path(private$.outdir, "consolidated.db"), extra_cols = FALSE)$mtime
    }
  )
  )

#' A convenience function for initialising [`Piecemeal`] objects.
#'
#' There is rarely a reason to attach \pkg{piecemeal} via [library()], so `piecemeal::init(outdir)` is provided as shorthand for [`Piecemeal`]`$new(outdir)`.
#' @param outdir the directory to hold the partial simulation results.
#' @return A [`Piecemeal`] object.
#' @seealso [`Piecemeal`]
#' @examples
#' outdir <- file.path(tempdir(), "piecemeal_demo")
#' sim <- piecemeal::init(outdir)
#' # a.k.a. piecemeal::Piecemeal$new(outdir)
#' # a.k.a.
#' # library(piecemeal)
#' # Piecemeal$new(outdir)
#' @export
init <- function(outdir) Piecemeal$new(outdir)

#' @noRd
#' @export
summary.Piecemeal <- function(object, ...) {
  object$status(...)
}

format_ago <- function(x) {
  if (is.na(x)) "never"
  else paste0(format(x, usetz = TRUE),
              " (", format(find_time_unit(Sys.time() - x), digits = 1), " ago)")
}

#' @noRd
#' @export
print.Piecemeal_status <- function(x, ...) {
  cat("A Piecemeal simulation\n")
  cat("Output directory:", attr(x, "outdir"), "\n\n")
  print(as.data.frame(x))

  cat("Last successful completion:", format_ago(attr(x, "last_OK")), "\n")
  cat("Last consolidation:", format_ago(attr(x, "last_consolidated")), "\n")

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
  # This works because "s" > "m" > "h" in the alphabet.
  units <- max(ifelse(is.na(dt) | dt < 60 * 2, "secs",
               ifelse(dt < 60 * 60 * 2, "mins",
                      "hours")))

  as.difftime(dt / c(secs = 1, mins = 60, hours = 60 * 60)[units],
              units = units)
}

find_rate_unit <- function(hz) {
  # This works because "s" > "m" > "h" > "d" in the alphabet.
  per <- min(ifelse(is.na(hz) | hz >= 1, "sec",
             ifelse(hz >= 1 / 60, "min",
             ifelse(hz >= 1 / 60 / 60, "hour",
                    "day"))))

  structure(hz * c(sec = 1, min = 60, hour = 60 * 60, day = 24 * 60 * 60)[per],
            per = per, class = "piecemeal_rate")
}

#' @noRd
#' @export
format.piecemeal_rate <- function(x, ...) {
  paste(format(as.numeric(x), ...), "per", attr(x, "per"))
}

#' @noRd
#' @export
print.Piecemeal_eta <- function(x, ...) {
  if (x$recent < 2L) {
    cli_alert_info("Too few runs completed: no ETA calculation possible.")
    return(invisible(x))
  }

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

  check_lock <- function(path) {
    lock <- filelock::lock(path, timeout = 0)

    # Lock attempt unsuccessful -> locked
    if (is.null(lock)) return(TRUE)

    # Delete and unlock -> unlocked
    unlink(path)
    filelock::unlock(lock)
    FALSE
  }

  ## filelock::lock() leaks file descriptors
  ## (https://github.com/r-lib/filelock/issues/28), so this is a
  ## workaround at least on Unix-alikes: the file descriptor will be
  ## closed with the child process.
  if (.Platform$OS.type == "unix")
    parallel::mccollect(parallel::mcparallel(check_lock(path)))[[1]]
  else check_lock(path)
}

# Empty result structure for missing/corrupted files
empty_result <- list(seed = NULL, treatment = NULL, output = NULL, fn = NULL, subdirs = NULL, OK = FALSE)

# Check if a result file is in the old format, warn the user, and convert.
convert_old_format <- function(o, fn) {
  if("config" %in% names(o)) {
    cli_warn(
      "{.file {fn}} is in the old (0.2) result format. Future versions of {.pkg piecemeal} may not support it."
    )
    utils::modifyList(o, o$config)
    o$config <- NULL
  }
  o
}

safe_readRDS <- function(file, ..., verbose = FALSE) {
  tryCatch(readRDS(file, ...),
           error = function(e) {
             if(verbose)
               cli_alert_warning("Run file {.file {file}} is corrupted. This should never happen.")
             empty_result
           }) |> convert_old_format(file)
}

run_config <- function(config, error, env = NULL) {
  env <- env %||% globalenv()

  if(error != ".debug") { # If debugging, just run the worker and return the result.
    outdir <- get(outdir_var, env)

    fn <- config$fn
    subdirs <- config$subdirs
    dn <- do.call(file.path, c(list(outdir), subdirs))
    fn <- file.path(dn, fn)

    # If this treatment + seed combination has been run, move on.
    if (file.exists(fn) || db_has_result(outdir, fn)) return(paste(fn, "SKIPPED", sep = "\n"))

    # Or, if it's already being run by another process, move on; otherwise, lock it.
    #
    # Here, the containing directory is created nonempty so that
    # file.remove() run by consolidate_results() cannot remove it before
    # lock() gets a chance to create a file in it. Once a lock file is
    # in place (if it wasn't already), we can remove the placeholder
    # subdirectory.
    dir.create(dl <- file.path(dn, "dirlock"), recursive = TRUE, showWarnings = FALSE)
    fnlock <- filelock::lock(paste0(fn, ".lock"), timeout = 0)
    suppressWarnings(try(unlink(dl, recursive = TRUE), silent = TRUE))
    if(is.null(fnlock)) return(paste(fn, "SKIPPED", sep = "\n"))
    on.exit({
      filelock::unlock(fnlock)
      unlink(paste0(fn, ".lock"))
    }, add = TRUE)
  }

  treatment <- config$treatment
  if(".seed" %in% names(formals(env[[worker_var]])))
    treatment$.seed <- config$seed

  # Save and (on exit) restore the random seed so that running the worker does
  # not affect the RNG state of the system.
  rng_state <- mget(".Random.seed", globalenv(), ifnotfound = list(NULL))[[1]]
  if (!is.null(rng_state)) on.exit(assign(".Random.seed", rng_state, globalenv()), add = TRUE)
  set.seed(config$seed)
  # Results data structure = config + output + OK flag.
  config$output <- switch(error,
                          stop =,
                          .debug = eval(as.call(c(list(as.name(worker_var)), treatment)), env),
                          try(eval(as.call(c(list(as.name(worker_var)), treatment)), env), silent = TRUE))

  if (error != ".debug") {
    if(inherits(config$output, "try-error")) {
      if(error == "skip") return(paste(fn, config$output, sep = "\n"))
      config$OK <- FALSE
    } else config$OK <- TRUE


    # saveRDS() is not atomic, whereas file.rename() typically is. The
    # following pattern guarantees that if the process is killed while
    # the results are being written out, a corrupted file does not
    # result.
    saveRDS(config, paste0(fn, ".tmp"))
    file.rename(paste0(fn, ".tmp"), fn)
    if (config$OK) {
      # Touch the file.
      file.create(file.path(outdir, "last_OK"))
      paste(fn, "OK", sep = "\n")
    } else {
      paste(fn, config$output, sep = "\n")
    }
  } else config
}
