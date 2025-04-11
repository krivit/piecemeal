#' @details This package grew out of a common problem of running a simulation with large numbers of treatment combinations and replications on a shared computing cluster. Using available tools such as \pkg{parallel} and even \CRANpkg{foreach} can be frustrating for a number of reasons:
#' * If any of the function runs results in an error, all results are lost.
#' * Tracking down which configuration resulted in an error and reproducing it can be frustrating, and if the fix does not fix all the errors, one has to start over.
#' * If one underestimates the amount of time all the jobs will take, all results are lost.
#' * Conversely, if the cluster is freer than anticipated, it is desirable to queue another job to get things done twice as fast.
#'
#' Those package with fault-tolerance capabilities typically focus on crashing worker nodes.
#'
#' This can be worked around in a variety of ways. Functions can be wrapped in [try()]. Rather than returning the result to the manager process, the worker can save its results to a unique file, with the results collated at the end. [`Piecemeal`] automates this and other tricks.
#'
#' See [the vignette `vignette("piecemeal")`](../doc/piecemeal.html) for a worked example.
"_PACKAGE"
