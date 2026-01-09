#' Quote PostgreSQL identifier safely using double quotes
#' Escapes '"' as '""'
#' @param x character vector
#' @return quoted identifiers
#' @export
quoteIdentPg <- function(x) {

  paste0('"', gsub('"', '""', x, fixed = TRUE), '"')
}
