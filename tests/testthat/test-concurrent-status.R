# This test requires precise timings, so skip if we can't control the
# execution environment.
skip_on_cran()

outdir <- tempfile("piecemeal_concurrent_test_")

# Function that takes 1 second to run.
f <- function(...) {
  Sys.sleep(1)
  TRUE
}

sim <- piecemeal::init(outdir)
sim$reset(confirm = FALSE)
# No parallel!
sim$worker(f)$nrep(3)

test_that("Checking status() while running", {
  # Run the function in a separate process.
  proc <- parallel::mcparallel(sim$run())

  Sys.sleep(0.5) # After 1/2 second, 1 running, 2 to do.
  expect_error(st <- sim$status(), NA)
  expect_equal(c(st), c("Running (approx.)" = 1L, ToDo = 2L))
  expect_null(attr(st, "eta"))
  expect_message(print(sim$eta()), "Too few runs completed: no ETA calculation possible.")

  Sys.sleep(1) # After 1 1/2 seconds, 1 done, 1 running, 1 to do.
  expect_error(st <- sim$status(), NA)
  expect_equal(c(st), c(Done = 1L, "Running (approx.)" = 1L, ToDo = 1L))
  expect_null(attr(st, "eta"))
  expect_message(print(sim$eta()), "Too few runs completed: no ETA calculation possible.")

  Sys.sleep(1) # After 2 1/2 seconds, 2 done, 1 running, and ETA available.
  expect_error(st <- sim$status(), NA)
  expect_equal(c(st), c(Done = 2L, "Running (approx.)" = 1L))
  expect_s3_class(eta <- attr(st, "eta"), "Piecemeal_eta")
  expect_equal(eta$cost, 1, tolerance = 0.05)
  expect_equal(eta$rate, 1, tolerance = 0.05)
  expect_equal(eta$left, 1, tolerance = 0.05)

  Sys.sleep(1) # After 3 1/2 seconds, all done, and ETA available.
  expect_error(st <- sim$status(), NA)
  expect_equal(c(st), c(Done = 3L))
  expect_s3_class(eta <- attr(st, "eta"), "Piecemeal_eta")
  expect_equal(eta$cost, 1, tolerance = 0.05)
  expect_equal(eta$rate, 1, tolerance = 0.05)
  expect_equal(eta$left, 0, tolerance = 0.05)

  # Should be OK and return TRUEs.
  res <- parallel::mccollect(proc)[[1]]
  expect_true(all(endsWith(res, "OK")))
  expect_equal(sim$result_df()[, 1], c(TRUE, TRUE, TRUE))
})

sim$reset(confirm = FALSE)
