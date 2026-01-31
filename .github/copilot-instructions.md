# GitHub Copilot Instructions for piecemeal

## Repository Overview

This is an R package called `piecemeal` that provides tools for managing large simulation studies with many independent replications and treatment configurations. The package is built around an R6 class that handles parallel processing, fault tolerance, and result collection.

## Code Style and Conventions

### R Package Structure
- Follow standard R package conventions as defined by R-CMD-check
- Use roxygen2 for documentation with markdown support enabled
- Package dependencies: R6, filelock, rlang, purrr
- Minimum R version: 4.2.0

### R6 Class Design
- The main class `Piecemeal` is defined in `R/Piecemeal.R`
- Use private fields (prefixed with `.`) for internal state
- Methods should return `self` (using `invisible(self)`) to enable method chaining
- Follow the existing pattern of method chaining: `sim$factorial()$nrep()$worker()$cluster()`

### Code Style
- Use 2-space indentation
- Prefer `<-` for assignment
- Use `::` for explicit package references in code
- Follow tidyverse style guide for R code
- Keep lines under 80 characters when practical

### Documentation
- All exported functions and methods must have roxygen2 documentation
- Use markdown in roxygen comments (enabled via `Roxygen: list(markdown = TRUE)`)
- Include `@examples` sections that demonstrate actual usage
- Reference related functions using markdown links: `[function_name()]`
- Suppress Rd file creation for unexported functions (via @noRd)

## Testing

### Framework
- Uses testthat 3.x edition for testing
- Tests are in `tests/testthat/` directory
- Test files should be named `test-*.R`
- Parallel testing is enabled

### Testing Pattern
```r
test_that("description of test", {
  outdir <- tempfile("piecemeal_test_")
  sim <- piecemeal::init(outdir)
  # ... test code ...
  unlink(outdir, recursive = TRUE)  # Clean up temp directory
})
```

### Key Testing Considerations
- Always use temporary directories for test outputs
- Clean up test artifacts with `unlink(outdir, recursive = TRUE)`
- Test both successful cases and error conditions
- Test with various treatment configurations and seed values

## Building and Checking

### Local Development
```r
# Document the package
devtools::document()

# Run tests
devtools::test()

# Check the package
devtools::check()
```

### CI/CD
- The package uses GitHub Actions with `R-CMD-check` workflow
- Tests run on multiple platforms: macOS, Windows, Ubuntu (devel, release, oldrel-1)
- Build arguments: `c("--no-manual","--compact-vignettes=gs+qpdf")`

## Key Features and Implementation Details

### Simulation Management
- Results are saved as individual RDS files in output directory
- File locking (via `filelock` package) prevents race conditions when multiple jobs run simultaneously
- Each simulation run is identified by a hash of its configuration and seed

### Parallel Processing
- Uses base R's `parallel` package with `clusterApplyLB()` for load balancing
- Worker nodes receive setup code, treatment configurations, and random seeds
- Errors in worker functions are caught and saved, not propagated

### Result Collection
- `$result_df()` method collates all results into a data frame
- Failed runs can be identified and debugged separately
- Simulation can be resumed after interruption

## Common Operations

### Adding New Methods
1. Add private helper methods in the `private = list()` section
2. Add public methods in the `public = list()` section
3. Document with roxygen2
4. Return `invisible(self)` for chainable methods
5. Add tests in `tests/testthat/`

### Modifying Existing Functionality
1. Check existing tests to understand expected behavior
2. Update implementation
3. Update or add tests as needed
4. Run `devtools::check()` to ensure package integrity
5. Update documentation if method signatures or behavior change

### Error Handling
- Use informative error messages with `stop()`
- Include context about which configuration caused the error
- Catch errors in worker functions and save them with results

## Dependencies

### Required Packages
- R6: For R6 class system
- filelock: For file locking mechanism
- rlang: For utilities (hashing, etc.)
- purrr: For functional programming tools

### When Adding Dependencies
- Add to `Imports` field in DESCRIPTION
- Use `@importFrom package function` in roxygen documentation
- Prefer explicit `package::function()` calls in examples

## File Locations

- Main code: `R/Piecemeal.R`
- Tests: `tests/testthat/test-*.R`
- Documentation: Roxygen comments in source files
- Vignettes: `vignettes/` directory
- Examples: `inst/examples/` directory

## Additional Notes

- The package name "piecemeal" refers to handling simulations piece by piece
- Target users are researchers running computationally intensive simulation studies
- The package is designed to work on shared computing clusters with queuing systems
