test_that("Piecemeal can be initialized and prints", {
  outdir <- tempfile("piecemeal_test_")
  sim <- Piecemeal$new(outdir)
  expect_s3_class(sim, "Piecemeal")
  expect_output(print(sim), "A Piecemeal simulation")
  unlink(outdir, recursive = TRUE)
})
