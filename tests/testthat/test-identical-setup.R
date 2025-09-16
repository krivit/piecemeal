test_that("Identical Piecemeal setups produce identical results", {
  outdir1 <- tempfile("piecemeal_id1_")
  outdir2 <- tempfile("piecemeal_id2_")
  worker_fun <- function(x, y, .seed) {
    set.seed(.seed)
    list(p = x * y, u = runif(1))
  }
  # Setup 1
  sim1 <- piecemeal::init(outdir1)
  sim1$factorial(x = 1:2, y = 3:4)$nrep(3)
  sim1$worker(worker_fun)
  sim1$run(shuffle = FALSE)
  df1 <- sim1$result_df()

  # Setup 2 (identical)
  sim2 <- piecemeal::init(outdir2)
  sim2$factorial(x = 1:2, y = 3:4)$nrep(3)
  sim2$worker(worker_fun)
  sim2$run(shuffle = FALSE)
  df2 <- sim2$result_df()

  # Sort for comparison
  df1 <- df1[order(df1$x, df1$y, df1$.seed), ]
  df2 <- df2[order(df2$x, df2$y, df2$.seed), ]

  expect_equal(df1$p, df2$p)
  expect_equal(df1$u, df2$u)
  expect_equal(df1$x, df2$x)
  expect_equal(df1$y, df2$y)
  expect_equal(df1$.seed, df2$.seed)

  unlink(outdir1, recursive = TRUE)
  unlink(outdir2, recursive = TRUE)
})
