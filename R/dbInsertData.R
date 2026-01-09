#' Insert Data into SQL Database
#'
#' Wrapper function that routes to the appropriate dialect-specific insert
#' function based on the `dialect` parameter.
#'
#' @param conn A valid database connection object.
#' @param new.data A non-empty `data.frame` to insert.
#' @param schema.name Schema name. Defaults to "dbo" for mssql, "public" for postgres.
#' @param table.name Target table name.
#' @param chunk.size Number of rows per chunk. Default is `10000`.
#' @param overwrite Whether to overwrite existing data. Default is `FALSE`.
#' @param verbose Whether to print progress messages. Default is `TRUE`.
#' @param dialect Character string specifying database dialect: "mssql" or "postgres".
#'
#' @return A `data.frame` transaction log.
#' @export
#'
#' @examples
#' \dontrun{
#' log.df <- dbInsertData(
#'   conn = conn,
#'   new.data = my.data.df,
#'   schema.name = "public",
#'   table.name = "events",
#'   dialect = "postgres"
#' )
#' }
dbInsertData <- function(
    conn,
    new.data,
    schema.name = NULL,
    table.name,
    chunk.size = 10000L,
    overwrite = FALSE,
    verbose = TRUE,
    dialect
) {
  if (!is.character(dialect) || length(dialect) != 1) {
    stop("Argument 'dialect' must be a character string")
  }
  if (!dialect %in% c("mssql", "postgres")) {
    stop("Argument 'dialect' must be 'mssql' or 'postgres', got: '", dialect, "'")
  }

  if (is.null(schema.name)) {
    schema.name <- switch(dialect,
                          mssql = "dbo",
                          postgres = "public"
    )
  }

  log.df <- switch(dialect,
                   mssql = dbInsertDataMssql(
                     conn = conn,
                     new.data = new.data,
                     schema.name = schema.name,
                     table.name = table.name,
                     chunk.size = chunk.size,
                     overwrite = overwrite,
                     verbose = verbose
                   ),
                   postgres = dbInsertDataPostgres(
                     conn = conn,
                     new.data = new.data,
                     schema.name = schema.name,
                     table.name = table.name,
                     chunk.size = chunk.size,
                     overwrite = overwrite,
                     verbose = verbose
                   )
  )

  return(log.df)
}
