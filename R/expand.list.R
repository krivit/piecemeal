expand.list <- function(...) {
  expand.grid(list(...), stringsAsFactors = FALSE, KEEP.OUT.ATTRS = FALSE) |> pmap(list)
}
