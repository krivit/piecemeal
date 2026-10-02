test_that("simulation alerts use cli messages", {
  outdir <- tempfile("piecemeal_test_")
  sim <- piecemeal::init(outdir)
  sim$worker(function() 1L)

  expect_message(sim$run(), "Starting 1 runs", class = "cliMessage")

  unlink(outdir, recursive = TRUE)
})

test_that("reserved seed errors retain their context", {
  outdir <- tempfile("piecemeal_test_")
  sim <- piecemeal::init(outdir)
  sim$worker(function(.seed) .seed)
  sim$treatments(list(list(.seed = 1L)))

  expect_error(sim$run(), "In treatment configuration 1")

  unlink(outdir, recursive = TRUE)
})
