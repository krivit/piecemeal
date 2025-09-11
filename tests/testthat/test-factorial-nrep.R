test_that("Piecemeal can set up factorial design and nrep", {
  outdir <- tempfile("piecemeal_test_")
  sim <- Piecemeal$new(outdir)
  sim$factorial(x = 1:2, y = 3:4)$nrep(2)
  todo <- sim$todo()
  expect_true(length(todo) == 8) # 2 x 2 x 2
  unlink(outdir, recursive = TRUE)
})
