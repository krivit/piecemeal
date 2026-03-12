test_that("Piecemeal runs a simple worker and collects results", {
  outdir <- tempfile("piecemeal_test_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:2)$nrep(2)
  sim$worker(function(a, .seed) a + .seed)

  # Before any run, last_OK file does not exist
  expect_true(is.na(sim$last_OK()))

  res <- sim$run(shuffle = FALSE)
  df <- sim$result_df()
  expect_equal(nrow(df), 4)
  expect_true(all(df$a %in% 1:2))
  expect_true(all(df$.seed %in% 1:2))

  # After a successful run, last_OK returns a recent timestamp
  t_ok <- sim$last_OK()
  expect_s3_class(t_ok, "POSIXct")
  expect_true(!is.na(t_ok))

  unlink(outdir, recursive = TRUE)
})
