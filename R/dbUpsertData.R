#' Upsert Data into SQL Database
#'
#' Wrapper function that routes to the appropriate dialect-specific upsert
#' function based on the `dialect` parameter.
#'
#' @inheritParams dbInsertData
#' @param key.cols Character vector of column names forming the primary key.
#' @param timestamp.col Column name used for conflict resolution.
#' @param keep "first" or "last"; whether to keep earliest or latest record on conflict.
#' @param check.schema Whether to validate schema compatibility. Default is `TRUE`.
#'
#' @return A `data.frame` transaction log.
#' @export
#'
#' @examples
#' \dontrun{
#' log.df <- dbUpsertData(
#'   conn = conn,
#'   new.data = events.df,
#'   table.name = "events",
#'   key.cols = "event_id",
#'   timestamp.col = "updated_at",
#'   dialect = "postgres"
#' )
#' }
dbUpsertData <- function(
    conn,
    new.data,
    schema.name = NULL,
    table.name,
    key.cols,
    timestamp.col,
    keep = "last",
    chunk.size = 10000L,
    check.schema = TRUE,
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
                   mssql = dbUpsertDataMssql(
                     conn = conn,
                     new.data = new.data,
                     schema.name = schema.name,
                     table.name = table.name,
                     key.cols = key.cols,
                     timestamp.col = timestamp.col,
                     keep = keep,
                     chunk.size = chunk.size,
                     check.schema = check.schema,
                     verbose = verbose
                   ),
                   postgres = dbUpsertDataPostgres(
                     conn = conn,
                     new.data = new.data,
                     schema.name = schema.name,
                     table.name = table.name,
                     key.cols = key.cols,
                     timestamp.col = timestamp.col,
                     keep = keep,
                     chunk.size = chunk.size,
                     check.schema = check.schema,
                     verbose = verbose
                   )
  )

  return(log.df)
}
