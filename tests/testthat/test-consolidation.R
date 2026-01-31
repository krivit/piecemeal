test_that("Consolidation stores and retrieves results correctly", {
  outdir <- tempfile("piecemeal_consolidate_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:2, b = 1:2)$nrep(2)
  sim$worker(function(a, b, .seed) list(result = a + b + .seed))

  # Run the simulation
  sim$run(shuffle = FALSE)

  # Check that results exist
  initial_files <- list.files(outdir, pattern = "\\.rds$", recursive = TRUE)
  expect_true(length(initial_files) > 0)

  # Get initial result count
  initial_results <- sim$result_list()
  initial_count <- length(initial_results)
  expect_equal(initial_count, 8) # 2x2x2

  # Consolidate the results
  count <- sim$consolidate(max_files = 100)
  expect_true(count > 0)

  # Check that files were removed
  remaining_files <- list.files(outdir, pattern = "\\.rds$", recursive = TRUE)
  remaining_files <- remaining_files[!grepl("consolidated\\.db", remaining_files)]
  expect_true(length(remaining_files) < length(initial_files))

  # Check that we can still read all results
  consolidated_results <- sim$result_list()
  expect_equal(length(consolidated_results), initial_count)

  # Check that results are identical
  for (i in seq_along(initial_results)) {
    expect_equal(initial_results[[i]]$seed, consolidated_results[[i]]$seed)
    expect_equal(initial_results[[i]]$treatment, consolidated_results[[i]]$treatment)
    expect_equal(initial_results[[i]]$output, consolidated_results[[i]]$output)
  }

  # Check that result_df still works
  df <- sim$result_df()
  expect_equal(nrow(df), 8)

  unlink(outdir, recursive = TRUE)
})

test_that("Consolidation only consolidates successful runs", {
  outdir <- tempfile("piecemeal_consolidate_error_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:3)$nrep(1)
  sim$worker(function(a) {
    if (a == 2) stop("Intentional error")
    list(result = a)
  })

  # Run the simulation (will have errors)
  suppressMessages(sim$run(shuffle = FALSE))

  # Count files before consolidation
  before_files <- list.files(outdir, pattern = "\\.rds$", recursive = TRUE)
  before_files <- before_files[!grepl("consolidated\\.db", before_files)]
  before_count <- length(before_files)
  expect_equal(before_count, 3) # 3 results (2 success, 1 error)

  # Consolidate
  count <- sim$consolidate(max_files = 100)
  expect_equal(count, 2) # Only 2 successful runs consolidated

  # Check that error file remains
  after_files <- list.files(outdir, pattern = "\\.rds$", recursive = TRUE)
  after_files <- after_files[!grepl("consolidated\\.db", after_files)]
  expect_equal(length(after_files), 1) # Only the error file remains

  # Check that we can still read all results (including the error)
  results <- sim$result_list()
  expect_equal(length(results), 3)

  # Check that the error is still accessible
  ok_count <- sum(sapply(results, function(r) r$OK))
  expect_equal(ok_count, 2)

  unlink(outdir, recursive = TRUE)
})

test_that("Consolidation locking prevents concurrent consolidation", {
  outdir <- tempfile("piecemeal_consolidate_lock_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:5)$nrep(1)
  sim$worker(function(a) list(result = a))

  # Run the simulation
  sim$run(shuffle = FALSE)

  # Create a lock manually
  lock_path <- file.path(outdir, ".consolidate.lock")
  lock <- filelock::lock(lock_path, timeout = 1000)  # Wait up to 1 second for lock

  # Verify lock was acquired
  expect_false(is.null(lock))

  # Try to consolidate (should fail due to lock)
  expect_message(sim$consolidate(max_files = 100), "Another process is consolidating")

  # Release the lock
  filelock::unlock(lock)
  unlink(lock_path)

  # Now consolidation should work
  count <- sim$consolidate(max_files = 100)
  expect_true(count > 0)

  unlink(outdir, recursive = TRUE)
})

test_that("max_files parameter limits consolidation", {
  outdir <- tempfile("piecemeal_consolidate_max_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:10)$nrep(1)
  sim$worker(function(a) list(result = a))

  # Run the simulation
  sim$run(shuffle = FALSE)

  # Consolidate only 5 files
  count <- sim$consolidate(max_files = 5)
  expect_equal(count, 5)

  # Check that 5 files remain
  remaining_files <- list.files(outdir, pattern = "\\.rds$", recursive = TRUE)
  remaining_files <- remaining_files[!grepl("consolidated\\.db", remaining_files)]
  expect_equal(length(remaining_files), 5)

  # Consolidate the rest
  count2 <- sim$consolidate(max_files = 100)
  expect_equal(count2, 5)

  # Check that all files are consolidated
  final_files <- list.files(outdir, pattern = "\\.rds$", recursive = TRUE)
  final_files <- final_files[!grepl("consolidated\\.db", final_files)]
  expect_equal(length(final_files), 0)

  # Verify all results are still accessible
  results <- sim$result_list()
  expect_equal(length(results), 10)

  unlink(outdir, recursive = TRUE)
})
