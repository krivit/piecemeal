test_that("autorun() respects forced and default options", {
  prior_option <- options("piecemeal.autorun")
  on.exit(do.call(options, prior_option))

  outdir <- tempfile("piecemeal_test_autorun_true_")
  on.exit(unlink(outdir, recursive = TRUE), add = TRUE)
  sim <- piecemeal::init(outdir)
  sim$worker(function() 1)

  options(piecemeal.autorun = TRUE)
  sim$autorun(shuffle = FALSE)
  expect_length(sim$result_list(), 1)

  outdir <- tempfile("piecemeal_test_autorun_false_")
  on.exit(unlink(outdir, recursive = TRUE), add = TRUE)
  sim <- piecemeal::init(outdir)
  sim$worker(function() 1)

  options(piecemeal.autorun = FALSE)
  sim$autorun(shuffle = FALSE)
  expect_length(sim$result_list(), 0)

  options(piecemeal.autorun = NA)
  sim$autorun(shuffle = FALSE)
  expect_length(sim$result_list(), 0)

  options(piecemeal.autorun = NULL)
  sim$autorun(shuffle = FALSE)
  expect_length(sim$result_list(), 0)
})
