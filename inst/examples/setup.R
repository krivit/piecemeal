#!/usr/bin/env Rscript

library(piecemeal)

# Piecemeal is an R6 class, so we access its methods using $.
# Initialise, with the output directory.
sim <- piecemeal::init(file.path(tempdir(), "piecemeal_demo"))

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

# Only print simulation status if using interactively or run from top
# level (not source()).
if (interactive() || sys.nframe() == 0) print(sim$status())
