#' Qualify schema + table name safely
#' @param schema schema name
#' @param table table name
#' @return [schema].[table]
qualifyTableMs <- function(schema, table) {
  paste0(quoteIdentMs(schema), ".", quoteIdentMs(table))
}
