test_that(".done_gen() yields individual run files one at a time", {
  outdir <- tempfile("piecemeal_test_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:2, b = 1:2)$nrep(2)
  sim$worker(function(a, b, .seed) list(result = a + b + .seed))
  sim$run(shuffle = FALSE)

  private <- sim$.__enclos_env__$private
  gen <- private$.done_gen()

  found <- character(0)
  repeat {
    f <- gen$next_file()
    if (is.null(f)) break
    # Each call returns exactly one file, and progress is
    # non-decreasing and consistent with what has been found so far.
    expect_length(f, 1)
    p <- gen$progress()
    expect_equal(p$found, length(found) + 1)
    expect_true(p$total >= p$found)
    found <- c(found, f)
  }

  # All 8 runs should have been found, matching the eager .done().
  expect_equal(length(found), 8)
  expect_setequal(found, private$.done(progress = FALSE))

  unlink(outdir, recursive = TRUE)
})

test_that(".done() matches .done_gen() ordering and includes consolidated runs", {
  outdir <- tempfile("piecemeal_test_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:2)$nrep(2)
  sim$worker(function(a, .seed) list(result = a + .seed))
  sim$run(shuffle = FALSE)

  before <- sim$result_list()
  expect_equal(length(before), 4)

  sim$consolidate()

  after <- sim$result_list()
  expect_equal(length(after), 4)
  for (i in seq_along(before)) {
    expect_equal(before[[i]]$seed, after[[i]]$seed)
    expect_equal(before[[i]]$treatment, after[[i]]$treatment)
    expect_equal(before[[i]]$output, after[[i]]$output)
  }

  unlink(outdir, recursive = TRUE)
})

test_that(".done_gen() handles an empty/nonexistent output directory", {
  outdir <- tempfile("piecemeal_test_")
  sim <- piecemeal::init(outdir)

  private <- sim$.__enclos_env__$private
  gen <- private$.done_gen()
  expect_null(gen$next_file())
  p <- gen$progress()
  expect_equal(p$found, 0)

  unlink(outdir, recursive = TRUE)
})
