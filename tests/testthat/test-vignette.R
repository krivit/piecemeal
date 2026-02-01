# Tests based on the vignette in vignettes/piecemeal.Rmd

o <- options(cli.default_handler = function(...) {})

# 1. Setup
outdir <- file.path(tempdir(), "piecemeal_vignette_test")
sim <- piecemeal::init(outdir)
sim$reset(confirm = FALSE)
sim$cluster(2)
sim$factorial(x = 2^(0:1), y = 3^(0:3))
sim$nrep(3)

# 2. Worker function with missing variable 'a'
f <- function(x, y) {
  p <- x*y
  u <- runif(1)
  errcond <- p + floor(u * 100) %% 10 + a
  if(errcond %% 4 == 0) stop("condition ", errcond %% 8, call. = FALSE)
  dbl(p = p, u = u)
}
sim$worker(f)

# 2.1 All runs should error due to missing 'a'
test_that("All runs error due to missing variable a", {
  sim$run()
  errs <- sim$erred()
  expect_length(errs, 24)
  expect_true(all(sapply(errs, function(e) grepl("object 'a' not found", as.character(e$output)))))
})

# 3. Export 'a', expect errors due to missing rlang
a <- 8
sim$export_vars("a")

test_that("All runs error due to missing rlang::dbl", {
  sim$run()
  errs <- sim$erred()
  expect_length(errs, 24)
  expect_true(any(sapply(errs, function(e) grepl("could not find function \\\"dbl\\\"", as.character(e$output)))))
})

# 4. Setup rlang, expect some errors due to function bug
sim$setup({library(rlang)})

test_that("Some runs succeed, some error due to function bug", {
  res <- sim$run()
  expect_length(grep("OK", res), 16)
  df <- sim$result_df()
  expect_equal(nrow(df), 16)
  errs <- sim$erred()
  expect_equal(length(errs), 8)
  expect_true(any(sapply(errs, function(e) grepl("condition", as.character(e$output)))))
})

# 5. Fix the function, expect all runs succeed
f_fixed <- function(x, y) {
  p <- x*y
  u <- runif(1)
  dbl(p = p, u = u)
}
sim$worker(f_fixed)

test_that("All runs succeed after fixing function", {
  restab <- capture.output(res <- sim$run(), type = "message")[4:6] |>
    textConnection() |> read.table(header = TRUE)
  expect_length(res, 8)
  expect_equal(restab, data.frame(row.names = as.character(1:2),
                                  Status = c("OK", "SKIPPED"),
                                  Runs = c(8, 16)))
  df <- sim$result_df()
  expect_equal(nrow(df), 24)
  expect_length(sim$erred(), 0)
})

# 6. Add more replications and run again
sim$nrep(5)

test_that("Additional replications are run", {
  res <- sim$run()
  expect_length(res, 16)
  df <- sim$result_df()
  expect_equal(nrow(df), 40)
})

# Clean up
test_that("Cleanup: reset deletes all results", {
  sim$reset(confirm = FALSE)
  expect_false(dir.exists(outdir))
})

options(o)
