#' A class for setting up and running large simulations piecemeal
#'
#' This class facilitates simulation studies made up of many relatively small replications and treatment configurations in a setting (such as a shared a computing cluster) in which the parallel processing may be interrupted due to time limits, errors, or for other reasons. Regular implementations, such as, \pkg{parallel}, execute the worker function for each configuration of interest and then return the result list. This means that if the process is interrupted due to, say, the job running out of time, all results are lost. `Piecemeal` instead saves the result of each treatment level and seed combination into its own file, then collates the results at the end.
#'
#'
#' @details A chain of `R6` method calls is used to specify the setup and the worker functions, the treatment configurations to be passed to the worker, and parallelism and other simulation settings. Then, when `$run()` is called, the cluster is started, worker nodes are initialised, and every combination of random seed and treatment configuration is passed to [clusterApplyLB()].
#'
#' On the worker nodes, the worker function is not called directly; rather, care is taken to make sure that the specified configuration and seed is not already being worked on. This makes it safe to, e.g., queue multiple jobs for the same simulation. If the configuration is available, `set.seed()` is called with the seed and then the worker function is run.
#'
#' Errors in the worker function are caught and error messages saved and returned.
#' 
#' @examples
#'
#' outdir <- file.path(tempdir(), "piecemeal_demo")
#'
#' # New simulation study.
#' runner <- Piecemeal$new(outdir)
#' runner$reset(FALSE) # Reset, just in case.
#' 
#' runner$ # Set up the following simulation study:
#' # a socket cluster with 2 nodes;
#'   cluster(2)$
#' # a factorial design with x = 1, 5, 25 and y = 1, 3, 9, 27;
#'   factorial(x = 5^(0:2), y = 3^(0:3))$
#' # with 2 replications per combination of x and y
#'   nrep(2)$
#' # calling a function of x and y, returning their product and a random number between 0 and 1;
#'   worker(function(x, y) c(prod = x*y, u = runif(1)))$
#' # and using one level of directory splitting.
#'   options(split = 1)
#'
#' # Run this setup; note the invisible output.
#' head(runner$run())
#' # Running again, already done.
#' head(runner$run())
#'
#' # Collate into a data frame.
#' (out <- runner$collate())
#'
#' runner$reset(FALSE) # Reset, just in case.
#'
#' # We can also run this without a cluster; this could be useful in debugging.
#' runner$cluster(NULL)
#' runner$run()
#' identical(out, runner$collate())
#' 
#' @import parallel
#' @importFrom rlang hash
#' @importFrom R6 R6Class
#' @export
Piecemeal <- R6Class("Piecemeal",
  private = list(
    .outdir = NULL,
    .cl_setup = NULL,
    .setup_expr = {},
    .treatments = list(),
    .worker = NULL,
    .seeds = NULL,
    .cl_vars = NULL,
    .split = 0L,
    .error = "skip"),

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
    send_vars = function(varlist, .add = TRUE) {
      if(!.add) private$.cl_vars <- NULL
      private$.cl_vars <- unique(c(private$.cl_vars, varlist))
      invisible(self)
    },

    #' @description Specify code to be run on each worker node at the start of the simulation.
    #' @param expr an expression; if passed, replaces the previous expression; if empty, resets it to nothing.
    setup = function(expr) {
      if(missing(expr)) private$.setup_expr <- {}
      private$.setup_expr <- substitute(expr)
      invisible(self)
    },

    #' @description Specify the function to be run for each treatment configuration.
    #' @param fun a function whose arguments are specified by `$treatments()` and `$factorial()`.
    worker = function(fun) {
      private$.worker <- fun
      invisible(self)
    },

    #' @description Specify a list of treatment configurations to be run.
    #' @param l a list, typically of lists of arguments to be passed to the function specified by `worker`; it is recommended that these be as compact as possible, since they are [`serialize`]d and sent to the worker node for every combination of treatment configuration and random seed.
    #' @param .add whether the new treatment configurations should be added to the current list (if `TRUE`, the default) or replace it (if `FALSE`.
    treatments = function(l, .add = TRUE) {
      l <- lapply(l, function(x) structure(x, hash = rlang::hash(x)))
      if(!.add) private$.treatments <- list()
      private$.treatments <- c(private$.treatments, l)
      invisible(self)
    },

    #' @description Specify a list of treatment configurations to be run in a factorial design.
    #' @param ... vectors or lists whose Cartesian product will added to the treatment list; it is recommended that these be as compact as possible, since they are [`serialize`]d and sent to the worker node for every combination of treatment configuration and random seed.
    #' @param .filter a function that takes the same arguments as worker and returns `FALSE` if the treatment configuration should be skipped; defaults to accepting all configurations.
    #' @param .add whether the new treatment configurations should be added to the current list (if `TRUE`, the default) or replace it (if `FALSE`.
    factorial = function(..., .filter = function(...) TRUE, .add = TRUE) {
      treatments <- expand.list(...)
      treatments <- treatments[vapply(treatments, function(x) do.call(.filter, x), FALSE)]
      self$treatments(treatments, .add = .add)
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
      if(shuffle) configs <- configs[sample.int(length(configs))]
      

      invisible(simplify2array(
        if(is.null(cl)) lapply(configs, run_config, split = private$.split, error = private$.error, worker = private$.worker, outdir = private$.outdir)
        else clusterApplyLB(cl, configs, run_config, split = private$.split, error = private$.error)
      ))
    },

    #' @description List the configurations still to be run.
    #' @return A list of lists with arguments to the worker functions.
    todo = function() {
      done <- basename(list.files(private$.outdir, ".*\\.rds", full.names = FALSE, recursive = TRUE))
      configs <- expand.list(seed = private$.seeds, treatment = private$.treatments)
      configfn <- vapply(configs, config_fn, "")
      configs <- Map(function(conf, fn) c(conf, list(fn = fn)), configs, configfn)
      configs[! configfn %in% done]
    },

    #' @description Scan through the results files and collate them into a data frame.
    #' @param trt_tf,out_tf functions that take the treatment configuration list and the output respectively, and return lists that can be used as data frame columns.
    #' @return A data frame with columns corresponding to the values returned by `trt_tf` and `out_tf`, with the following additional columns:
    #' \describe{
    #' \item{`.seed`}{the random seed used.}
    #' \item{`.trt_hash`}{the hash of the treatment configuration.}
    #' \item{`.rds`}{the path to the RDS file}
    #' }
    collate = function(trt_tf = identity, out_tf = identity) {
      done <- list.files(private$.outdir, ".*\\.rds", full.names = TRUE, recursive = TRUE)

      l <- lapply(done, function(fn) {
        o <- try(readRDS(fn))
        if(o$OK)
          c(list(),
            trt_tf(o$config$treatment), out_tf(o$output),
            list(.seed = o$config$seed, .trt_hash = attr(o$config$treatment, "hash"), .rds = fn))
        else NULL
      })
      erred <- vapply(l, is.null, FALSE)
      if(any(erred)) message(sum(erred), " configurations have returned an error.")
      l <- l[!erred]
      do.call(rbind, l)
    },

    #' @description Clear the simulation results so far.
    #' @param confirm whether the user should be prompted to confirm deletion.
    reset = function(confirm = interactive()) {
      if(confirm){
        ans <- readline(paste0('This will delete all files from ', sQuote(private$.outdir), '. Are you sure? ("yes" to confirm, anything else to cancel) '))
        if(ans != "yes"){
          message("Cancelled.")
          return()
        }
      }
      unlink(private$.outdir, recursive = TRUE)
    },

    #' @description Set miscellaneous options.
    #' @param split whether the output files should be split up into subdirectories and how deeply; this can improve performance on some file systems.
    #' @param error how to handle worker errors:\describe{
    #' \item{`"skip"`}{return the error message as a part of `run()`'s return value, but do not save the RDS file; the next `run()` will attempt to run the worker for that configuration and seed again;}
    #' \item{`"save"`}{save the seed, the configuration, and the status, preventing future runs until the file is cleared.}
    #' }
    options = function(split = 0L, error = c("skip", "save")) {
      if(!missing(split)) private$.split = split
      if(!missing(error)) private$.error = match.arg(error)
    }
    
  )
  )

config_fn <- function(config, split = 0L) {
  paste(attr(config$treatment, "hash"), config$seed, "rds", sep = ".")
}

run_config <- function(config, split, error, worker = NULL, outdir = NULL) {
  if(is.null(worker)) worker <- get(".worker", .GlobalEnv)
  if(is.null(outdir)) outdir <- get(".outdir", .GlobalEnv)

  fn <- config$fn
  subdirs <- if(split) sapply(seq_len(split), function(i) substr(fn, i*2L - 1L, i*2L))
  dn <- do.call(file.path, c(list(outdir), subdirs))
  dir.create(dn, recursive = TRUE, showWarnings = FALSE)
  fn <- file.path(dn, fn)

  if(file.exists(fn)) return(paste(fn, "SKIPPED")) # If this treatment + seed combination has been run, move on.

  # Or, if it's already being run by another process, move on; otherwise, lock it.
  fnlock <- filelock::lock(fn, timeout = 0)
  on.exit(filelock::unlock(fnlock))
  if(is.null(fnlock)) return(paste(fn, "SKIPPED"))

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
  return(paste(fn, "OK"))
}
