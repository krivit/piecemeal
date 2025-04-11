#' The `Piecemeal` [`R6`] Class
#'
#' @description This class exports methods for configuring a simulation, running it, debugging failed configurations, and resuming the simulation. See [the vignette `vignette("piecemeal")`](../doc/piecemeal.html) for a worked example.
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
#'
#' # See vignette("piecemeal").
#'
#' @import parallel
#' @importFrom rlang hash
#' @import purrr
#' @importFrom R6 R6Class
#' @export
Piecemeal <- R6Class("Piecemeal",
  private = list(
    .outdir = NULL,
    .cl_setup = NULL,
    .setup = {},
    .treatments = list(),
    .worker = NULL,
    .seeds = NULL,
    .cl_vars = NULL,
    .split = c(1L, 1L),
    .error = "skip",
    .done = function() list.files(private$.outdir, ".*\\.rds", full.names = TRUE, recursive = TRUE)
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

    #' @description Specify variables to be copied from the manager node to the worker nodes' global environment.
    #' @param varlist a character vector with variable names.
    #' @param .add whether the new variables should be added to the current list (if `TRUE`, the default) or replace it (if `FALSE`).
    export_vars = function(varlist, .add = TRUE) {
      if(!.add) private$.cl_vars <- NULL
      private$.cl_vars <- unique(c(private$.cl_vars, varlist))
      invisible(self)
    },

    #' @description Specify code to be run on each worker node at the start of the simulation.
    #' @param expr an expression; if passed, replaces the previous expression; if empty, resets it to nothing.
    setup = function(expr) {
      if(missing(expr)) private$.setup <- {}
      private$.setup <- substitute(expr)
      invisible(self)
    },

    #' @description Specify the function to be run for each treatment configuration.
    #' @param fun a function whose arguments are specified by `$treatments()` and `$factorial()`; if it has `.seed` as a named argument, the seed will be passed as well.
    worker = function(fun) {
      private$.worker <- fun
      invisible(self)
    },

    #' @description Specify a list of treatment configurations to be run.
    #' @param l a list, typically of lists of arguments to be passed to the function specified by `worker`; it is recommended that these be as compact as possible, since they are [`serialize`]d and sent to the worker node for every combination of treatment configuration and random seed.
    #' @param .add whether the new treatment configurations should be added to the current list (if `TRUE`, the default) or replace it (if `FALSE`.
    treatments = function(l, .add = TRUE) {
      l <- lapply(l, add_hash)
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

    #' @description Specify a number of replications for each treatment configuration.
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
      cl <- private$.cl_setup
      if(!is.null(cl) && !is(cl, "cluster")) {
        cl <- do.call(makeCluster, cl)
        on.exit(stopCluster(cl))
      }

      if(!is.null(cl)) {
        .worker <- private$.worker
        .outdir <- private$.outdir
        clusterExport(cl, c(".worker", ".outdir"), environment())

        if(length(private$.cl_vars)) clusterExport(cl, private$.cl_vars)

        clusterCall(cl, eval, private$.setup, envir = .GlobalEnv)
      }

      configs <- self$todo()
      message(sprintf("Starting %d runs. (%d already done.)", length(configs), done <- attr(configs, "done")))

      if(shuffle) configs <- configs[sample.int(length(configs))]

      statuses <- simplify2array(
        if(is.null(cl)) lapply(configs, run_config, error = private$.error, worker = private$.worker, outdir = private$.outdir)
        else clusterApplyLB(cl, configs, run_config, error = private$.error)
      )

      c(
        if(length(statuses)) statuses |> strsplit("\n", fixed = TRUE) |> vapply(`[`, "", -1L),
        rep("SKIPPED", done)
      ) |>
        table() |> as.data.frame() |> setNames(c("Status", "Runs")) |>
        capture.output() |> paste(collapse = "\n") |> message()

      if(length(statuses)) invisible(statuses) else character(0)
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
    #' @return A list of lists containing the contents of the result files.
    #' \describe{
    #' \item{`config`}{list of configuration settings, including the random seed and the output file name}
    #' \item{`treatment`}{arguments passed to the worker}
    #' \item{`OK`}{whether the worker succeeded or produced an error}
    #' \item{`rds`}{the path to the RDS file}
    #' }
    result_list = function() {
      lapply(private$.done(), function(fn) {
        o <- safe_readRDS(fn, verbose = TRUE)
        list(config = o$config[names(o$config) != "treatment"], output = o$output, treatment = o$config$treatment, OK = o$OK, rds = fn)
      })
    },

    #' @description Scan through the results files and collate them into a data frame.
    #' @param trt_tf,out_tf functions that take the treatment configuration list and the output respectively, and return lists that can be used as data frame columns.
    #' @return A data frame with columns corresponding to the values returned by `trt_tf` and `out_tf`, with the following additional columns:
    #' \describe{
    #' \item{`.seed`}{the random seed used.}
    #' \item{`.trt_hash`}{the hash of the treatment configuration.}
    #' \item{`.rds`}{the path to the RDS file}
    #' }
    #' Runs that erred are filtered out.
    result_df = function(trt_tf = identity, out_tf = identity) {
      l <- self$result_list() |>
        compact(\(x) x$config)

      OK <- map_lgl(l, "OK")
      if(!all(OK)) message(sprintf("%d/%d runs had returned an error.", sum(!OK), length(OK)))
      l <- l[OK]

      map(l, function(o) {
        as.data.frame(c(list(),
                        trt_tf(o$treatment), out_tf(o$output),
                        list(.seed = o$config$seed, .trt_hash = attr(o$treatment, "hash"), .rds = o$rds)))
      }) |>
        do.call(rbind, args = _)
    },

    #' @description Clear the simulation results so far.
    #' @param confirm whether the user should be prompted to confirm deletion.
    reset = function(confirm = interactive()) {
      if(confirm){
        ans <- readline(paste0('This will delete all files from ', sQuote(private$.outdir), '. Are you sure? ("yes" to confirm, anything else to cancel) '))
        if(ans != "yes"){
          message("Cancelled.")
          invisible(self)
        }
      }
      unlink(private$.outdir, recursive = TRUE)
      invisible(self)
    },

    #' @description Delete the result files for which the worker function failed and/or for which the files were corrupted.
    clean = function() {
      done <- private$.done()
      OK <- done |> map(safe_readRDS) |> map_lgl("OK")
      unlink(done[!OK])
      message(sprintf("%d/%d runs deleted.", sum(!OK), length(OK)))
      invisible(self)
    },

    #' @description List the configurations for which the worker function failed.
    erred = function() {
      private$.done() |>
        map(safe_readRDS) |>
        map(\(o) if(!o$OK) o$config) |>
        compact()
    },

    #' @description Set miscellaneous options.
    #' @param split a two-element vector indicating whether the output files should be split up into subdirectories and how deeply, the first for splitting configurations and the second for splitting seeds; this can improve performance on some file systems.
    #' @param error how to handle worker errors:\describe{
    #' \item{`"skip"`}{return the error message as a part of `run()`'s return value, but do not save the RDS file; the next `run()` will attempt to run the worker for that configuration and seed again;}
    #' \item{`"save"`}{save the seed, the configuration, and the status, preventing future runs until the file is cleared.}
    #' }
    options = function(split = c(1L, 1L), error = c("skip", "save")) {
      if(!missing(split)) private$.split <- rep_len(split, 2L)
      if(!missing(error)) private$.error <- match.arg(error)
      invisible(self)
    }
    
  )
  )

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

safe_readRDS <- function(file, ..., verbose = FALSE) {
  tryCatch(readRDS(file, ...),
           error = function(e) {
             if(verbose) message("Run file ", sQuote(file), " is corrupted. This should never happen.")
             list(config = NULL, output = NULL, OK = FALSE)
           })
}

run_config <- function(config, error, worker = NULL, outdir = NULL) {
  if(is.null(worker)) worker <- get(".worker", .GlobalEnv)
  if(is.null(outdir)) outdir <- get(".outdir", .GlobalEnv)

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

  if(".seed" %in% names(formals(worker)))
    config$treatment$.seed <- config$seed

  set.seed(config$seed)
  out <- try(do.call(worker, config$treatment))
  if(inherits(out, "try-error")){
    if(error == "skip") return(paste(fn, out, sep = "\n"))
    OK <- FALSE
  }else OK <- TRUE
  

  # saveRDS() is not atomic, whereas file.rename() typically is. The
  # following pattern guarantees that if the process is killed while
  # the results are being written out, a corrupted file does not
  # result.
  saveRDS(list(output = out, config = config, OK = OK), paste0(fn, ".tmp"))
  file.rename(paste0(fn, ".tmp"), fn)
  if(OK) return(paste(fn, "OK", sep = "\n")) else return(paste(fn, out, sep = "\n"))
}
