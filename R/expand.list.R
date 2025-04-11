#' @useDynLib piecemeal
expand.list <- function(...) {
  .Call("cartesian", lapply(list(...), as.list), PACKAGE = "piecemeal")
}
