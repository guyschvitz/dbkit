#' Insert Data into SQL Server in Chunks with Transaction Support
#'
#' Inserts a `data.frame` into a SQL Server table in chunks. Each chunk is
#' written inside a transaction savepoint so that errors in one chunk do not
#' corrupt previous ones. If any chunk fails, the entire operation is rolled
#' back. If all succeed, the transaction is committed.
#'
#' The function returns a transaction log as a `data.frame`. Each row records
#' a chunk insert or transaction event.
#'
#' Transaction log columns:
#' \describe{
#'   \item{operation}{Type of operation ("insert", "commit", "rollback").}
#'   \item{chunk}{Chunk number (if applicable).}
#'   \item{rows}{Number of rows inserted or affected.}
#'   \item{start_row}{First row index of the chunk (if applicable).}
#'   \item{end_row}{Last row index of the chunk (if applicable).}
#'   \item{status}{Operation status ("success" or "failed").}
#'   \item{message}{Details or error messages.}
#'   \item{timestamp}{Time the operation was logged (POSIXct, UTC).}
#' }
#'
#' @param conn A valid SQL Server connection object (from \pkg{DBI}).
#' @param data.df A non-empty `data.frame` to insert.
#' @param table.id A `DBI::Id` object specifying the target table, or a character
#'   string for temporary tables.
#' @param chunk.size Integer, number of rows per chunk. Default is `10000`.
#' @param overwrite Logical; whether to overwrite an existing table. Default is `FALSE`.
#' @param verbose Logical; whether to print progress messages. Default is `TRUE`.
#'
#' @return A `data.frame` transaction log.
#' @export
#'
#' @importFrom DBI dbIsValid dbBegin dbCommit dbRollback dbExecute dbWriteTable
#' @importFrom glue glue
#'
#' @examples
#' \dontrun{
#' log.df <- dbInsertDataSqlServer(
#'   conn = con,
#'   data.df = new.data,
#'   table.id = DBI::Id(schema = "dbo", table = "events"),
#'   chunk.size = 5000
#' )
#' }
dbInsertDataSqlServer <- function(
    conn,
    data.df,
    table.id,
    chunk.size = 10000L,
    overwrite = FALSE,
    verbose = TRUE
) {
  if (!DBI::dbIsValid(conn)) stop("Database connection is not valid")
  if (!is.data.frame(data.df) || nrow(data.df) == 0) stop("data.df must be a non-empty data frame")
  chunk.size <- as.integer(chunk.size)
  if (is.na(chunk.size) || chunk.size <= 0) stop("Argument 'chunk.size' must be a positive integer")

  total.rows <- nrow(data.df)
  num.chunks <- ceiling(total.rows / chunk.size)
  log.df <- data.frame()

  if (verbose) {
    message(glue::glue("Inserting {format(total.rows, big.mark = ',')} records in {num.chunks} chunks"))
  }

  DBI::dbBegin(conn)

  tryCatch({
    for (i in seq_len(num.chunks)) {
      start.row <- (i - 1L) * chunk.size + 1L
      end.row <- min(i * chunk.size, total.rows)
      chunk.df <- data.df[start.row:end.row, , drop = FALSE]
      savepoint.name <- glue::glue("chunk_{i}")
      DBI::dbExecute(conn, glue::glue("SAVE TRANSACTION {savepoint.name}"))

      tryCatch({
        DBI::dbWriteTable(
          conn,
          table.id,
          chunk.df,
          overwrite = (i == 1L && overwrite),
          append = (i > 1L || !overwrite)
        )

        DBI::dbExecute(conn, glue::glue("COMMIT TRANSACTION {savepoint.name}"))

        if (verbose) {
          message(glue::glue("Inserted chunk {i}/{num.chunks} (rows {format(start.row)}-{format(end.row)})"))
        }

        log.df <- rbind(log.df, data.frame(
          operation = "insert",
          chunk = i,
          rows = nrow(chunk.df),
          start_row = start.row,
          end_row = end.row,
          status = "success",
          message = "chunk committed",
          timestamp = Sys.time()
        ))
      }, error = function(e) {
        DBI::dbExecute(conn, glue::glue("ROLLBACK TRANSACTION {savepoint.name}"))
        log.df <- rbind(log.df, data.frame(
          operation = "insert",
          chunk = i,
          rows = nrow(chunk.df),
          start_row = start.row,
          end_row = end.row,
          status = "failed",
          message = conditionMessage(e),
          timestamp = Sys.time()
        ))
        stop(glue::glue("Failed to insert chunk {i}:\n{conditionMessage(e)}"))
      })
    }

    DBI::dbCommit(conn)
    log.df <- rbind(log.df, data.frame(
      operation = "commit",
      chunk = NA,
      rows = total.rows,
      start_row = 1,
      end_row = total.rows,
      status = "success",
      message = "all chunks committed",
      timestamp = Sys.time()
    ))

    return(log.df)
  }, error = function(e) {
    DBI::dbRollback(conn)
    log.df <- rbind(log.df, data.frame(
      operation = "rollback",
      chunk = NA,
      rows = 0,
      start_row = NA,
      end_row = NA,
      status = "failed",
      message = conditionMessage(e),
      timestamp = Sys.time()
    ))
    return(log.df)
  })
}
