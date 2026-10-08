test_that("run() does not stop a preexisting cluster", {
  skip_on_os("windows")

  outdir <- tempfile("piecemeal_test_cluster_")
  on.exit(unlink(outdir, recursive = TRUE))

  cl <- parallel::makeCluster(1, type = "FORK")
  on.exit(parallel::stopCluster(cl), add = TRUE)

  sim <- piecemeal::init(outdir)
  sim$worker(function() 1)
  sim$cluster(cl)
  sim$run(shuffle = FALSE)

  expect_length(sim$result_list(), 1)
  expect_identical(parallel::clusterCall(cl, function() TRUE), list(TRUE))
})
