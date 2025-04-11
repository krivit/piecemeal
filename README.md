
# piecemeal

<!-- badges: start -->
[![R-CMD-check](https://github.com/krivit/piecemeal/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/krivit/piecemeal/actions/workflows/R-CMD-check.yaml)
[![Codecov test coverage](https://codecov.io/gh/krivit/piecemeal/graph/badge.svg)](https://app.codecov.io/gh/krivit/piecemeal)
[![CRAN status](https://www.r-pkg.org/badges/version/piecemeal)](https://CRAN.R-project.org/package=piecemeal)
<!-- badges: end -->

This package grew out of a common problem of running a simulation with large numbers of treatment combinations and replications on a shared computing cluster. Using available tools such as \pkg{parallel} and even \CRANpkg{foreach} can be frustrating for a number of reasons:

* If any of the function runs results in an error, all results are lost.
* Tracking down which configuration resulted in an error and reproducing it can be frustrating, and if the fix does not fix all the errors, one has to start over.
* If one underestimates the amount of time all the jobs will take, all results are lost.
* Conversely, if the cluster is freer than anticipated, it is desirable to queue another job to get things done twice as fast.

Those package with fault-tolerance capabilities typically focus on crashing worker nodes.

This can be worked around in a variety of ways. Functions can be wrapped in [try()]. Rather than returning the result to the manager process, the worker can save its results to a unique file, with the results collated at the end. [`Piecemeal`] automates this, and keeps careful track of inputs and random seeds, ensuring that problematic realisations can be located and debugged quickly and efficiently. A locking system even makes it possible to have multiple jobs running the same study on the same cluster without interfering with each other.

## Installation

You can install the development version of `piecemeal` from [GitHub](https://github.com/) with:

``` r
# install.packages("remotes")
remotes::install_github("krivit/piecemeal")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(piecemeal)

# Initialise, with the output directory.
sim <- Piecemeal$new(file.path(tempdir(), "piecemeal_demo"))
# Clear the previous simulation, if present.
sim$reset(FALSE)

# Set up a simulation:
sim$
  # for every combination of x = 1, 2 and y = 1, 3, 9, 27,
  factorial(x = 2^(0:1), y = 3^(0:3))$
  # each replicated 3 times,
  nrep(3)$
  # first load library 'rlang',
  setup({library(rlang)})$
  # then for each x, y, and seed, evaluate
  worker(function(x, y) {
    p <- x*y
    u <- runif(1)
    dbl(p = p, u = u)
  })$
  # on a cluster with two nodes.
  cluster(2)

# Go!
sim$run()

# Get a table with the results.
sim$result_df()
```

