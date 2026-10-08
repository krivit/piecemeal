# Tests based on the vignette in vignettes/piecemeal.Rmd

o <- options(cli.default_handler = function(...) {})
had_seed <- exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE)
if (had_seed) old_seed <- get(".Random.seed", envir = .GlobalEnv)
set.seed(1)
if (exists("a", inherits = FALSE)) rm(a)

# 1. Setup
outdir <- tempfile("piecemeal_vignette_test_")
sim <- piecemeal::init(outdir)
sim$reset(confirm = FALSE)
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

sort_results <- function(results) {
  results <- results[order(results$x, results$y, results$.seed),
                     c("x", "y", "p", "u", ".seed")]
  rownames(results) <- NULL
  results
}

test_that("setup and todo match the vignette", {
  setup <- paste(capture.output(print(sim)), collapse = "\n")
  expect_match(setup, "Design:\\s*8 treatment configurations by 3 seeds = 24 runs")
  expect_match(setup, "Ready to execute\\?\\s*Yes\\.")

  todo <- head(sim$todo(), 2)
  expect_equal(vapply(todo, `[[`, numeric(1), "seed"), c(1, 2))
  expect_equal(
    lapply(todo, function(config) unlist(config$treatment[c("x", "y")])),
    list(c(x = 1, y = 1), c(x = 1, y = 1))
  )
})

# 2.1 Test-run the worker before exporting 'a'
test_that("test run reports the missing variable a", {
  expect_error(sim$test(shuffle = FALSE), "object 'a' not found")
})

# 3. Export 'a', expect errors due to missing rlang
a <- 8
sim$export_vars("a")

test_that("test run reports the missing rlang function", {
  expect_error(sim$test(shuffle = FALSE), 'could not find function "dbl"')
})

# 4. Setup rlang, expect some errors due to function bug
sim$setup({library(rlang)})

test_that("test run succeeds after setup", {
  result <- sim$test(shuffle = FALSE)
  expect_equal(unname(result[[1]]$output["p"]), 1)
  expect_equal(unname(round(result[[1]]$output["u"], 7)), 0.2655087)
})

test_that("Some runs succeed, some error due to function bug", {
  res <- sim$run()
  expect_length(grep("OK", res), 16)
  df <- sim$result_df()
  expect_equal(nrow(df), 16)
  expected <- expand.grid(x = c(1, 2), y = c(1, 3, 9, 27), .seed = 1:3)
  expected$p <- expected$x * expected$y
  expected$u <- c(0.2655087, 0.1848823, 0.1680415)[expected$.seed]
  expected <- expected[!(expected$x == 2 & expected$.seed %in% c(1, 3)), ]
  expected <- expected[c("x", "y", "p", "u", ".seed")]
  expect_equal(
    transform(sort_results(df), u = round(u, 7)),
    transform(sort_results(expected), u = round(u, 7))
  )

  expect_message(skipped <- sim$run(), "SKIPPED: 24")
  expect_length(skipped, 0)

  status <- sim$status()
  expect_equal(as.integer(status), c(16L, 4L, 4L))
  expect_equal(names(status), c("Done", "Error : condition 0", "Error : condition 4"))

  files <- list.files(outdir, recursive = TRUE)
  expect_equal(sum(grepl("\\.rds$", files)), 24)
  expect_true("last_OK" %in% files)

  errs <- sim$erred()
  expect_equal(length(errs), 8)
  error_keys <- vapply(
    errs,
    function(e) paste(e$treatment$x, e$treatment$y, e$seed),
    character(1)
  )
  expected_errors <- expand.grid(y = c(1, 3, 9, 27), seed = c(1, 3))
  expect_setequal(error_keys, paste(2, expected_errors$y, expected_errors$seed))
  expect_true(all(vapply(errs, function(e) {
    condition <- if (e$treatment$y %in% c(1, 9)) "condition 0" else "condition 4"
    grepl(condition, as.character(e$output))
  }, logical(1))))
})

# 5. Fix the function, expect all runs succeed
f_fixed <- function(x, y) {
  p <- x*y
  u <- runif(1)
  dbl(p = p, u = u)
}
sim$worker(f_fixed)

test_that("All runs succeed after fixing function", {
  expect_message(
    res <- sim$run(),
    "Run summary:.*OK: 8.*SKIPPED: 16"
  )
  expect_length(res, 8)
  df <- sim$result_df()
  expect_equal(nrow(df), 24)
  expected <- expand.grid(x = c(1, 2), y = c(1, 3, 9, 27), .seed = 1:3)
  expected$p <- expected$x * expected$y
  expected$u <- c(0.2655087, 0.1848823, 0.1680415)[expected$.seed]
  expected <- expected[c("x", "y", "p", "u", ".seed")]
  expect_equal(
    transform(sort_results(df), u = round(u, 7)),
    transform(sort_results(expected), u = round(u, 7))
  )
  expect_length(sim$erred(), 0)
})

# 6. Consolidate successful runs
test_that("consolidation preserves the demonstrated results", {
  sim$consolidate()
  expect_setequal(
    list.files(outdir, recursive = TRUE),
    c("consolidated.db", "last_OK")
  )
  expect_equal(nrow(sim$result_df()), 24)
})

# 6. Add more replications and run again
sim$nrep(5)

test_that("ETA is available after the initial runs", {
  eta <- sim$eta()
  expect_s3_class(eta, "Piecemeal_eta")
  expect_gt(eta$recent, 1)
  expect_gte(eta$left, 0)
})

test_that("Additional replications are run", {
  res <- sim$run()
  expect_length(res, 16)
  df <- sim$result_df()
  expect_equal(nrow(df), 40)
  expected <- expand.grid(x = c(1, 2), y = c(1, 3, 9, 27), .seed = 1:5)
  expected$p <- expected$x * expected$y
  expected$u <- c(
    0.2655087, 0.1848823, 0.1680415, 0.5858003, 0.2002145
  )[expected$.seed]
  expected <- expected[c("x", "y", "p", "u", ".seed")]
  expect_equal(
    transform(sort_results(df), u = round(u, 7)),
    transform(sort_results(expected), u = round(u, 7))
  )
})

# Clean up
test_that("Cleanup: reset deletes all results", {
  sim$reset(confirm = FALSE)
  expect_false(dir.exists(outdir))
})

options(o)
if (had_seed) {
  assign(".Random.seed", old_seed, envir = .GlobalEnv)
} else if (exists(".Random.seed", envir = .GlobalEnv, inherits = FALSE)) {
  rm(".Random.seed", envir = .GlobalEnv)
}
