test_that("check_args() detects treatment arguments the worker cannot accept", {
  outdir <- tempfile("piecemeal_test_args_")
  on.exit(unlink(outdir, recursive = TRUE))

  sim <- piecemeal::init(outdir)
  sim$treatments(list(list(a = 1, unexpected = 2)))
  sim$worker(function(a) a)

  expect_error(
    sim$.__enclos_env__$private$.check_args(),
    "unused argument.*treatment configuration 1"
  )
})

test_that("check_args() validates treatments when the worker accepts .seed", {
  outdir <- tempfile("piecemeal_test_args_seed_")
  on.exit(unlink(outdir, recursive = TRUE))

  sim <- piecemeal::init(outdir)
  sim$treatments(list(list(a = 1, unexpected = 2)))
  sim$worker(function(a, .seed) a + .seed)

  expect_error(
    sim$.__enclos_env__$private$.check_args(),
    "unused argument.*treatment configuration 1"
  )
})

test_that("run() does not stop a preexisting cluster", {
  outdir <- tempfile("piecemeal_test_cluster_")
  on.exit(unlink(outdir, recursive = TRUE))

  cl <- parallel::makeCluster(1)
  on.exit(parallel::stopCluster(cl), add = TRUE)

  sim <- piecemeal::init(outdir)
  sim$worker(function() 1)
  sim$seeds(integer())
  sim$cluster(cl)
  sim$run(shuffle = FALSE)

  expect_identical(parallel::clusterCall(cl, function() TRUE), list(TRUE))
})

test_that("result_list() handles corrupted result files", {
  for (contents in list(raw(), as.raw(rep(0, 8)))) {
    outdir <- tempfile("piecemeal_test_corrupt_")
    on.exit(unlink(outdir, recursive = TRUE), add = TRUE)

    sim <- piecemeal::init(outdir)
    sim$worker(function() 1)
    sim$run(shuffle = FALSE)

    result_file <- list.files(
      outdir, pattern = "\\.rds$", full.names = TRUE, recursive = TRUE
    )
    expect_length(result_file, 1)
    writeBin(contents, result_file)

    result <- sim$result_list()
    expect_length(result, 1)
    expect_false(result[[1]]$OK)
    expect_null(result[[1]]$treatment)
    expect_null(result[[1]]$output)
  }
})

test_that("result_list() and result_df() apply treatment and output transforms", {
  outdir <- tempfile("piecemeal_test_transforms_")
  on.exit(unlink(outdir, recursive = TRUE))

  sim <- piecemeal::init(outdir)
  sim$factorial(a = 2)
  sim$worker(function(a) list(value = a + 1))
  sim$run(shuffle = FALSE)

  trt_tf <- function(x) list(double_a = 2 * x$a)
  out_tf <- function(x) list(scaled_value = 10 * x$value)

  result <- sim$result_list(trt_tf = trt_tf, out_tf = out_tf)[[1]]
  expect_equal(result$treatment, list(double_a = 4))
  expect_equal(result$output, list(scaled_value = 30))

  df <- sim$result_df(trt_tf = trt_tf, out_tf = out_tf)
  expect_equal(df$double_a, 4)
  expect_equal(df$scaled_value, 30)
})

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
