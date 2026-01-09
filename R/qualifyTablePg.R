#' Qualify schema + table name safely for PostgreSQL
#' @param schema schema name
#' @param table table name
#' @return "schema"."table"
#' @export
qualifyTablePg <- function(schema, table) {
  paste0(quoteIdentPg(schema), ".", quoteIdentPg(table))
}
