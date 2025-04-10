
# piecemeal

<!-- badges: start -->
[![R-CMD-check](https://github.com/krivit/piecemeal/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/krivit/piecemeal/actions/workflows/R-CMD-check.yaml)
[![Codecov test coverage](https://codecov.io/gh/krivit/piecemeal/graph/badge.svg)](https://app.codecov.io/gh/krivit/piecemeal)
[![CRAN status](https://www.r-pkg.org/badges/version/piecemeal)](https://CRAN.R-project.org/package=piecemeal)
<!-- badges: end -->

The goal of `piecemeal` is to simplify running simulation studies made up of many relatively small replications and treatment configurations in a setting (such as a shared a computing cluster) in which the parallel processing may be interrupted due to time limits, errors, or for other reasons. Regular implementations, such as, `parallel`, execute the worker function for each configuration of interest and then return the result list, often failing even if one of the workers reports an error. This means that if the process is interrupted due to, say, the job running out of time or a stochastic bug, all results are lost. `piecemeal` instead saves the result of each treatment level and seed combination into its own file, then collates the results at the end.

It keeps careful track of inputs and random seeds, ensuring that problematic realisations can be located and debugged quickly and efficiently. A locking system even makes it possible to have multiple jobs running the same study on the same cluster without interfering with each other.

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
example(Piecemeal)
```

