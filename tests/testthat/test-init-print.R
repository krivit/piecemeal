test_that("Piecemeal can be initialized and prints", {
  outdir <- tempfile("piecemeal_test_")
  sim <- piecemeal::init(outdir)
  expect_s3_class(sim, "Piecemeal")
  expect_output(print(sim), "A Piecemeal simulation")
  unlink(outdir, recursive = TRUE)
})

test_that("print.Piecemeal_status formats the table with percentage, total, and no row numbers", {
  outdir <- tempfile("piecemeal_test_print_")
  sim <- piecemeal::init(outdir)
  sim$factorial(a = 1:2)$nrep(2)
  sim$worker(function(a, .seed) list(result = a + .seed))
  sim$run(shuffle = FALSE)

  out <- capture.output(print(sim$status()))

  # No row numbers: lines with data should not start with a digit
  data_lines <- grep("Done|ToDo|Total|Running", out, value = TRUE)
  expect_false(any(grepl("^\\s*[0-9]+\\s", data_lines)),
               info = "Row numbers should not appear")

  # Percentage column header
  expect_true(any(grepl("Percentage", out)),
              info = "Percentage column header should be present")

  # Total row
  expect_true(any(grepl("Total", out)),
              info = "Total row should be present")

  # Percentages match expected format (e.g. "100%", "50%")
  expect_true(any(grepl("[0-9.]+%", out)),
              info = "Percentage values should appear")

  unlink(outdir, recursive = TRUE)
})
