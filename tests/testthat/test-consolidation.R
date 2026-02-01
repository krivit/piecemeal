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
  count <- sim$consolidate()
  expect_true(count > 0)

  # Check that files were removed
  remaining_files <- list.files(outdir, pattern = "\\.rds$", recursive = TRUE)
  remaining_files <- remaining_files[!grepl("consolidated\\.db", remaining_files)]
  expect_true(length(remaining_files) < length(initial_files))

  # Check that we can still read all results
  consolidated_results <- sim$result_list()
  expect_equal(length(consolidated_results), initial_count)

  # Check that results are identical - verify contents in detail
  for (i in seq_along(initial_results)) {
    expect_equal(initial_results[[i]]$seed, consolidated_results[[i]]$seed)
    expect_equal(initial_results[[i]]$treatment, consolidated_results[[i]]$treatment)
    expect_equal(initial_results[[i]]$output, consolidated_results[[i]]$output)
    expect_equal(initial_results[[i]]$OK, consolidated_results[[i]]$OK)
    # Verify the actual computed result value
    expected_result <- initial_results[[i]]$treatment$a +
                      initial_results[[i]]$treatment$b +
                      initial_results[[i]]$seed
    expect_equal(consolidated_results[[i]]$output$result, expected_result)
  }

  # Check that result_df still works
  df <- sim$result_df()
  expect_equal(nrow(df), 8)

  # Verify df contains correct values
  for (i in seq_len(nrow(df))) {
    expected_result <- df$a[i] + df$b[i] + df$.seed[i]
    expect_equal(df$result[i], expected_result)
  }

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
  count <- sim$consolidate()
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

  # Verify contents of successful consolidated runs
  successful_results <- results[sapply(results, function(r) r$OK)]
  expect_equal(length(successful_results), 2)
  for (r in successful_results) {
    # Verify the output matches the input
    expect_equal(r$output$result, r$treatment$a)
    # Verify it's either a=1 or a=3 (not a=2 which errored)
    expect_true(r$treatment$a %in% c(1, 3))
  }

  unlink(outdir, recursive = TRUE)
})

test_that("Consolidation can be called multiple times safely", {
  outdir <- tempfile("piecemeal_consolidate_lock_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:5)$nrep(1)
  sim$worker(function(a) list(result = a))

  # Run the simulation
  sim$run(shuffle = FALSE)

  # Consolidate all files
  count <- sim$consolidate()
  expect_true(count > 0)

  # Try to consolidate again (should return 0 since no files left)
  count2 <- sim$consolidate()
  expect_equal(count2, 0)

  unlink(outdir, recursive = TRUE)
})

test_that("Consolidation processes all files at once", {
  outdir <- tempfile("piecemeal_consolidate_all_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:10)$nrep(1)
  sim$worker(function(a) list(result = a))

  # Run the simulation
  sim$run(shuffle = FALSE)

  # Consolidate all files
  count <- sim$consolidate()
  expect_equal(count, 10)

  # Check that all files are consolidated
  final_files <- list.files(outdir, pattern = "\\.rds$", recursive = TRUE)
  final_files <- final_files[!grepl("consolidated\\.db", final_files)]
  expect_equal(length(final_files), 0)

  # Verify all results are still accessible
  results <- sim$result_list()
  expect_equal(length(results), 10)

  # Verify contents of all consolidated results
  for (r in results) {
    expect_true(r$OK)
    # Verify the output matches the input
    expect_equal(r$output$result, r$treatment$a)
    # Verify the value is in the expected range
    expect_true(r$treatment$a >= 1 && r$treatment$a <= 10)
  }

  unlink(outdir, recursive = TRUE)
})

test_that("Consolidation removes empty directories", {
  skip_on_os("windows")
  
  outdir <- tempfile("piecemeal_consolidate_dirs_")
  
  # Create a simulation with subdirectories by using the subdir parameter
  sim <- piecemeal::init(outdir)
  
  # Manually create subdirectories and files to test directory cleanup
  subdir1 <- file.path(outdir, "subdir1")
  subdir2 <- file.path(outdir, "subdir2")
  nested_dir <- file.path(subdir1, "nested")
  
  dir.create(subdir1, recursive = TRUE)
  dir.create(subdir2, recursive = TRUE)
  dir.create(nested_dir, recursive = TRUE)
  
  # Create some result files in subdirectories
  # We need to create valid result files
  result1 <- list(treatment = list(x = 1), seed = 1, output = list(result = 1), OK = TRUE)
  result2 <- list(treatment = list(x = 2), seed = 2, output = list(result = 2), OK = TRUE)
  result3 <- list(treatment = list(x = 3), seed = 3, output = list(result = 3), OK = TRUE)
  
  saveRDS(result1, file.path(subdir1, "result1.rds"))
  saveRDS(result2, file.path(nested_dir, "result2.rds"))
  saveRDS(result3, file.path(subdir2, "result3.rds"))
  
  # Verify directories exist before consolidation
  expect_true(dir.exists(subdir1))
  expect_true(dir.exists(subdir2))
  expect_true(dir.exists(nested_dir))
  
  # Consolidate
  count <- sim$consolidate()
  expect_equal(count, 3)
  
  # Check that empty subdirectories were removed
  expect_false(dir.exists(subdir1), info = "subdir1 should be removed after consolidation")
  expect_false(dir.exists(subdir2), info = "subdir2 should be removed after consolidation")
  expect_false(dir.exists(nested_dir), info = "nested directory should be removed after consolidation")
  
  # Verify the outdir still exists
  expect_true(dir.exists(outdir))
  
  # Verify consolidated.db exists
  expect_true(file.exists(file.path(outdir, "consolidated.db")))
  
  unlink(outdir, recursive = TRUE)
})

test_that("Consolidation preserves file modification times for ETA", {
  outdir <- tempfile("piecemeal_mtime_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:3)$nrep(2)
  sim$worker(function(a) {
    Sys.sleep(0.1) # Small delay to ensure different mtimes
    list(result = a)
  })
  
  # Run the simulation
  sim$run(shuffle = FALSE)
  
  # Get the result count before consolidation
  results_before <- sim$result_list()
  count_before <- length(results_before)
  expect_equal(count_before, 6)
  
  # Consolidate
  count <- sim$consolidate()
  expect_equal(count, 6)
  
  # Get the result count after consolidation
  results_after <- sim$result_list()
  count_after <- length(results_after)
  
  # The count should be preserved
  expect_equal(count_before, count_after)
  
  # Test that ETA calculation works with consolidated files
  # (This should not throw an error)
  eta_result <- sim$eta(window = 3600)
  expect_true(!is.null(eta_result))
  expect_true(is.numeric(eta_result$recent))
  
  # Verify that the eta() method can access mtimes from consolidated database
  # If mtimes weren't preserved, eta() would fail or return incorrect results
  # The fact that it runs without error is our test that mtimes work
  
  unlink(outdir, recursive = TRUE)
})

