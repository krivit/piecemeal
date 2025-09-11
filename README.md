
# piecemeal

<!-- badges: start -->
[![R-CMD-check](https://github.com/krivit/piecemeal/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/krivit/piecemeal/actions/workflows/R-CMD-check.yaml)
[![Codecov test coverage](https://codecov.io/gh/krivit/piecemeal/graph/badge.svg)](https://app.codecov.io/gh/krivit/piecemeal)
[![CRAN status](https://www.r-pkg.org/badges/version/piecemeal)](https://CRAN.R-project.org/package=piecemeal)
<!-- badges: end -->

This package addresses a common problem of running a simulation with large numbers of treatment combinations and replications on a shared computing cluster. The usual tools such as {parallel} and even {foreach} can be frustrating for a number of reasons:

* If any of the function runs results in an error, all results are lost.
* Tracking down which configuration resulted in an error and reproducing it can be frustrating, and if the fix does not fix all the errors, one has to start over.
* If one underestimates the amount of time all the jobs will take, all results are lost.
* Conversely, if the cluster is freer than anticipated, it's too late to queue another job to get things done twice as fast.

This can be worked around in a variety of ways. Functions can be wrapped in `try()`. Simulation inputs and random seed can be stored alongside the output. Rather than returning the result to the manager process, the worker can save its results to a unique file, with the results collated at the end. {piecemeal} automates this. Here are some of its features:

* It encapsulates simulation settings, from the experiment design to the cluster settings to which variables need to be sent to the worker nodes in a single object: you specify what you need, then just tell it to run, and it executes your instructions.
* The individual run results are accessible---but only if you want to see them.
* It keeps track of simulation configurations and random seeds, so if some run has failed, you can quickly identify and debug it.
* A locking mechanism makes it so that you can run multiple jobs for the same simulation study on the same host without them interfering with each other.
* Saving each run's results is atomic: either the run's result is fully saved, or it's not saved at all.
* You can check in on the progress of the simulation even while it's running: collate the results thus far, see which runs have produced errors and how, estimate the time left, etc..

## Installation

{piecemeal} is not (yet?) on CRAN. It can be installed from [GitHub](https://github.com/) with:
``` r
# install.packages("remotes")
remotes::install_github("krivit/piecemeal", build_vignettes = TRUE)
```
The current version does not need compilation, so it can be installed on systems without development tools.

## Example

This is a basic example. For a more involved example, including debuging the simulation, see the vignette (`vignette("piecemeal")`).

``` r
library(piecemeal)

# Piecemeal is an R6 class, so we access its methods using $.
# Initialise, with the output directory.
sim <- Piecemeal$new(file.path(tempdir(), "piecemeal_demo"))
# Clear the previous simulation, if present.
sim$reset()

# Set up a simulation:
sim$
  # for every combination of x = 1, 2 and y = 1, 3, 9, 27,
  factorial(x = 2^(0:1), y = 3^(0:3))$
  # each combination replicated 3 times (with seeds 1:3),
  nrep(3)$
  # first load library 'rlang',
  setup({library(rlang)})$
  # then for each x, y, and seed, evaluate
  worker(function(x, y) {
    p <- x*y
    u <- runif(1)
    dbl(p = p, u = u) # from rlang
  })$
  # on a cluster with two nodes.
  cluster(2)

# Go!
sim$run()

# Get a table with the results.
sim$result_df()
```

When running on a cluster, you may want to put the code for setting-up, the code for running, and the code for postprocessing into separate files. See [inst/examples](inst/examples) for an example.
