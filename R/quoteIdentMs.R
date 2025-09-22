#' Quote SQL Server identifier safely using []
#' Escapes ']' as ']]'
#' @param x character vector
#' @return quoted identifiers
quoteIdentMs <- function(x) {
  paste0("[", gsub("]", "]]", x, fixed = TRUE), "]")
}
