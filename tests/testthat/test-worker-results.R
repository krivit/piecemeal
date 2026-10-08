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
    expect_equal(unname(sim$status()["Corrupted"]), 1)
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
