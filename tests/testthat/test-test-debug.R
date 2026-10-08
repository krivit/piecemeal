test_that("test() runs requested configuration formats", {
  outdir <- tempfile("piecemeal_test_debug_")
  on.exit(unlink(outdir, recursive = TRUE))

  sim <- piecemeal::init(outdir)
  sim$factorial(x = 1:2)$nrep(2)
  sim$worker(function(x, .seed) x + .seed)
  todo <- sim$todo()

  explicit <- sim$test(config = todo[1], shuffle = FALSE)
  expect_equal(explicit[[1]]$output, 2)

  indexed <- sim$test(config = 2, shuffle = FALSE)
  expect_equal(indexed[[1]]$output, 3)

  set.seed(1)
  sampled <- sim$test(config = 2)
  expect_length(sampled, 2)
  expect_true(all(vapply(sampled, function(x) x$OK, logical(1))))

  hash_seed <- c(
    attr(todo[[1]]$treatment, "hash"),
    as.character(todo[[1]]$seed)
  )
  by_hash <- sim$test(config = hash_seed)
  expect_equal(by_hash[[1]]$output, 2)

  expect_error(sim$test(config = list("invalid")), "Invalid configuration")
  expect_error(sim$test(config = hash_seed[1]), "Invalid configuration")
})

test_that("test() restores error options after worker errors", {
  outdir <- tempfile("piecemeal_test_debug_")
  on.exit(unlink(outdir, recursive = TRUE))

  sim <- piecemeal::init(outdir)
  sim$factorial(x = 1)
  sim$worker(function(x) stop("test worker failure"))
  prior_error <- getOption("error")

  expect_error(sim$test(error = NULL), "test worker failure")
  expect_identical(getOption("error"), prior_error)
})

test_that("debug() runs explicit and saved failed configurations", {
  outdir <- tempfile("piecemeal_test_debug_")
  on.exit(unlink(outdir, recursive = TRUE))

  sim <- piecemeal::init(outdir)
  sim$factorial(x = 1:2)
  sim$worker(function(x) {
    if (x == 2) stop("test worker failure")
    x
  })
  todo <- sim$todo()

  explicit <- sim$debug(result = todo[[1]], error = NULL)
  expect_equal(explicit$output, 1)
  expect_error(sim$debug(result = "invalid"), "Invalid result")

  sim$run(shuffle = FALSE)
  failed <- sim$erred()
  expect_length(failed, 1)
  expect_error(sim$debug(result = 1, error = NULL), "test worker failure")
})
