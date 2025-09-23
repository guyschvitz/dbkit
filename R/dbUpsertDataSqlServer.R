#' Upsert Data to SQL Server with MERGE and Chunking
#'
#' Efficiently upserts large data frames into a SQL Server table using
#' `MERGE` and chunked inserts. If the target table does not exist, it is
#' created. Conflicts on key columns are resolved by comparing timestamps,
#' keeping either the earliest or latest record.
#'
#' The function returns a transaction log as a `data.frame`. Each row records
#' an operation: initial insert, merge, or cleanup.
#'
#' Transaction log columns:
#' \describe{
#'   \item{operation}{Type of operation ("insert_new_table", "merge", "cleanup").}
#'   \item{chunk}{Chunk number (if applicable).}
#'   \item{rows}{Number of rows inserted, updated, or processed.}
#'   \item{start_row}{First row index of the chunk (if applicable).}
#'   \item{end_row}{Last row index of the chunk (if applicable).}
#'   \item{status}{Operation status ("success" or "failed").}
#'   \item{message}{Details, e.g. "inserted=100, updated=50".}
#'   \item{timestamp}{Time the operation was logged (POSIXct, UTC).}
#' }
#'
#' @param conn A valid SQL Server connection object (from \pkg{DBI}).
#' @param new.data A non-empty `data.frame` with new data to upsert.
#' @param schema.name Character string naming the target schema (default: `"dbo"`).
#' @param table.name Character string naming the target table.
#' @param key.cols Character vector of column names forming the primary key.
#' @param timestamp.col Character scalar, the column used for conflict resolution.
#' @param keep `"first"` or `"last"`; whether to keep the earliest or latest record
#'   on timestamp conflicts. Default is `"last"`.
#' @param chunk.size Integer, number of rows per insert chunk. Default is `10000`.
#' @param check.schema Logical; whether to validate schema compatibility. Default is `TRUE`.
#' @param verbose Logical; whether to print progress messages. Default is `TRUE`.
#'
#' @return A `data.frame` transaction log.
#' @export
#'
#' @importFrom DBI dbIsValid Id dbExistsTable dbGetQuery dbRemoveTable
#' @importFrom glue glue
#'
#' @examples
#' \dontrun{
#' log.df <- dbUpsertDataSqlServer(
#'   conn = con,
#'   new.data = data.df,
#'   schema.name = "dbo",
#'   table.name = "events",
#'   key.cols = "event_id",
#'   timestamp.col = "updated_at"
#' )
#' }
dbUpsertDataSqlServer <- function(
    conn,
    new.data,
    schema.name,
    table.name,
    key.cols,
    timestamp.col,
    keep = "last",
    chunk.size = 10000L,
    check.schema = TRUE,
    verbose = TRUE
) {
  if (!DBI::dbIsValid(conn)) stop("Database connection is not valid")
  if (!is.data.frame(new.data) || nrow(new.data) == 0) stop("new.data must be a non-empty data frame")
  if (!is.character(key.cols) || length(key.cols) == 0) stop("key.cols must be non-empty character vector")
  if (!timestamp.col %in% names(new.data)) stop(glue::glue("Timestamp column '{timestamp.col}' not found"))
  if (!keep %in% c("first", "last")) stop("keep must be 'first' or 'last'")

  log.df <- data.frame()
  if (is.null(schema.name)) {
    schema.name <- "dbo"
  }

  # Force timestamps into UTC
  if (inherits(new.data[[timestamp.col]], "POSIXt")) {
    attr(new.data[[timestamp.col]], "tzone") <- "UTC"
  }

  table.id <- DBI::Id(schema = schema.name, table = table.name)
  table.exists <- DBI::dbExistsTable(conn, table.id)

  # Properly quoted target table string
  table.str <- qualifyTableMs(schema.name, table.name)

  if (!table.exists) {
    rows.inserted <- dbInsertDataSqlServer(
      conn = conn,
      new.data = new.data,
      schema.name = schema.name,
      table.name = table.name,
      chunk.size = chunk.size,
      overwrite = TRUE,
      verbose = verbose
    )

    log.df <- rbind(log.df, data.frame(
      operation = "insert_new_table",
      chunk = NA,
      rows = nrow(new.data),
      start_row = 1,
      end_row = nrow(new.data),
      status = "success",
      message = "table created and data inserted",
      timestamp = Sys.time()
    ))
  }

  # Staging (temp) table name
  temp.table.name <- sprintf("%s%s", table.name, format(Sys.time(), "%Y%m%d%H%M"))
  temp.table.id <- DBI::Id(schema = schema.name, table = temp.table.name)
  temp.table.str <- qualifyTableMs(schema.name, temp.table.name)

  # Create temp table as an empty copy of the target table to ensure schema compatibility
  create.temp.sql <- glue::glue("
    SELECT TOP 0 *
    INTO {temp.table.str}
    FROM {table.str}
  ")

  tryCatch({
    DBI::dbExecute(conn, glue::glue("DROP TABLE IF EXISTS {temp.table.str}"))
    DBI::dbExecute(conn, create.temp.sql)
    if (verbose) message(glue::glue("Created temporary table {temp.table.str} as empty copy of {table.str}"))
  }, error = function(e) {
    stop(glue::glue("Failed to create temporary table: {e$message}"))
  })

  # Insert data into the temp table
  dbInsertDataSqlServer(
    conn = conn,
    new.data = new.data,
    schema.name = schema.name,
    table.name = temp.table.name,
    chunk.size = chunk.size,
    overwrite = TRUE,
    verbose = verbose
  )

  timestamp.comparison <- if (keep == "last") ">" else "<"

  # Quote column identifiers
  join.condition <- paste(
    sprintf("target.%s = source.%s", quoteIdentMs(key.cols), quoteIdentMs(key.cols)),
    collapse = " AND "
  )
  non.key.cols <- setdiff(names(new.data), key.cols)
  update.clause <- paste(
    sprintf("target.%s = source.%s", quoteIdentMs(non.key.cols), quoteIdentMs(non.key.cols)),
    collapse = ", "
  )
  col.list <- paste(quoteIdentMs(names(new.data)), collapse = ", ")
  source.col.list <- paste(paste0("source.", quoteIdentMs(names(new.data))), collapse = ", ")

  merge.sql <- glue::glue("
    MERGE {table.str} AS target
    USING {temp.table.str} AS source
    ON {join.condition}
    WHEN MATCHED AND source.{quoteIdentMs(timestamp.col)} {timestamp.comparison} target.{quoteIdentMs(timestamp.col)}
      THEN UPDATE SET {update.clause}
    WHEN NOT MATCHED BY TARGET
      THEN INSERT ({col.list})
      VALUES ({source.col.list})
    OUTPUT $action AS action_type;
  ")

  merge.result <- DBI::dbGetQuery(conn, merge.sql)

  total.inserted <- sum(merge.result$action_type == "INSERT", na.rm = TRUE)
  total.updated <- sum(merge.result$action_type == "UPDATE", na.rm = TRUE)

  log.df <- rbind(log.df, data.frame(
    operation = "merge",
    chunk = NA,
    rows = nrow(new.data),
    start_row = 1,
    end_row = nrow(new.data),
    status = "success",
    message = glue::glue("inserted={total.inserted}, updated={total.updated}"),
    timestamp = Sys.time()
  ))

  if (DBI::dbExistsTable(conn, temp.table.id)) {
    DBI::dbRemoveTable(conn, temp.table.id)
    log.df <- rbind(log.df, data.frame(
      operation = "cleanup",
      chunk = NA,
      rows = 0,
      start_row = NA,
      end_row = NA,
      status = "success",
      message = "temporary table removed",
      timestamp = Sys.time()
    ))
  }

  return(log.df)
}
