#' Insert Data into SQL Server in Chunks with Transaction Support
#'
#' Inserts a `data.frame` into a SQL Server table in chunks. All chunks are
#' written within a single transaction. If any chunk fails, the entire operation
#' is rolled back. If all chunks succeed, the transaction is committed.
#'
#' The function returns a transaction log as a `data.frame`. Each row records
#' a chunk insert or transaction event.
#'
#' Transaction log columns:
#' \describe{
#'   \item{operation}{Type of operation ("insert", "commit", "rollback", "delete").}
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
#' @param new.data A non-empty `data.frame` to insert.
#' @param schema.name A non-empty character string specifying the target schema name.
#' @param table.name A non-empty character string specifying the target table name.
#' @param chunk.size Integer, number of rows per chunk. Default is `10000`.
#' @param overwrite Logical; whether to overwrite an existing table. Default is `FALSE`.
#' @param verbose Logical; whether to print progress messages. Default is `TRUE`.
#'
#' @return A `data.frame` transaction log.
#' @export
#'
#' @importFrom DBI dbIsValid dbBegin dbCommit dbRollback dbExecute dbWriteTable dbExistsTable Id
#' @importFrom glue glue
#'
#' @examples
#' \dontrun{
#' log.df <- dbInsertDataSqlServer(
#'   conn = con,
#'   new.data = new.data,
#'   schema.name = "dbo",
#'   table.name = "events",
#'   chunk.size = 5000
#' )
#' }
dbInsertDataSqlServer <- function(
    conn,
    new.data,
    schema.name,
    table.name,
    chunk.size = 10000L,
    overwrite = FALSE,
    verbose = TRUE
) {
  # Parameter validation
  if (!DBI::dbIsValid(conn)) stop("Database connection is not valid")
  if (!is.data.frame(new.data) || nrow(new.data) == 0) stop("new.data must be a non-empty data frame")
  if (!is.character(schema.name) || length(schema.name) != 1 || nchar(schema.name) == 0) {
    stop("schema.name must be a non-empty character string")
  }
  if (!is.character(table.name) || length(table.name) != 1 || nchar(table.name) == 0) {
    stop("table.name must be a non-empty character string")
  }
  chunk.size <- as.integer(chunk.size)
  if (is.na(chunk.size) || chunk.size <= 0) stop("Argument 'chunk.size' must be a positive integer")

  # Construct table identifiers
  table.id <- DBI::Id(schema = schema.name, table = table.name)
  table.str <- qualifyTableMs(schema.name, table.name)

  total.rows <- nrow(new.data)
  num.chunks <- ceiling(total.rows / chunk.size)
  log.df <- data.frame()

  if (verbose) {
    message(glue::glue("Inserting {format(total.rows, big.mark = ',')} records in {num.chunks} chunks to {table.str}"))
  }

  # Start main transaction
  DBI::dbBegin(conn)

  # Track if we need to rollback
  transaction.failed <- FALSE
  error.message <- NULL

  # Handle overwrite by deleting existing data
  if (overwrite && !transaction.failed) {
    if (DBI::dbExistsTable(conn, table.id)) {
      if (verbose) {
        message("Overwrite=TRUE: clearing existing table data")
      }

      result <- try({
        rows.deleted <- DBI::dbExecute(conn, glue::glue("DELETE FROM {table.str}"))

        if (verbose) {
          message(glue::glue("Deleted {format(rows.deleted, big.mark = ',')} existing rows"))
        }

        log.df <- rbind(log.df, data.frame(
          operation = "delete",
          chunk = NA,
          rows = rows.deleted,
          start_row = NA,
          end_row = NA,
          status = "success",
          message = "existing data cleared",
          timestamp = Sys.time(),
          stringsAsFactors = FALSE
        ))
        rows.deleted
      }, silent = TRUE)

      if (inherits(result, "try-error")) {
        transaction.failed <- TRUE
        error.message <- paste("Failed to clear existing table data:", attr(result, "condition")$message)
      }
    } else if (verbose) {
      message("Table doesn't exist, will be created on first chunk")
    }
  }

  # Insert data in chunks
  if (!transaction.failed) {
    for (i in seq_len(num.chunks)) {
      start.row <- (i - 1L) * chunk.size + 1L
      end.row <- min(i * chunk.size, total.rows)
      chunk.df <- new.data[start.row:end.row, , drop = FALSE]

      result <- try({
        DBI::dbWriteTable(
          conn,
          table.id,
          chunk.df,
          overwrite = FALSE,
          append = TRUE
        )

        if (verbose) {
          message(glue::glue("Inserted chunk {i}/{num.chunks} (rows {format(start.row, big.mark = ',')}-{format(end.row, big.mark = ',')})"))
        }

        log.df <<- rbind(log.df, data.frame(
          operation = "insert",
          chunk = i,
          rows = nrow(chunk.df),
          start_row = start.row,
          end_row = end.row,
          status = "success",
          message = "chunk inserted",
          timestamp = Sys.time(),
          stringsAsFactors = FALSE
        ))
        TRUE
      }, silent = TRUE)

      if (inherits(result, "try-error")) {
        transaction.failed <- TRUE
        error.message <- paste(glue::glue("Failed to insert chunk {i}:"), attr(result, "condition")$message)

        log.df <- rbind(log.df, data.frame(
          operation = "insert",
          chunk = i,
          rows = nrow(chunk.df),
          start_row = start.row,
          end_row = end.row,
          status = "failed",
          message = error.message,
          timestamp = Sys.time(),
          stringsAsFactors = FALSE
        ))
        break
      }
    }
  }

  # Commit or rollback based on success
  if (!transaction.failed) {
    DBI::dbCommit(conn)
    log.df <- rbind(log.df, data.frame(
      operation = "commit",
      chunk = NA,
      rows = total.rows,
      start_row = 1,
      end_row = total.rows,
      status = "success",
      message = "all chunks committed",
      timestamp = Sys.time(),
      stringsAsFactors = FALSE
    ))

    if (verbose) {
      message(glue::glue("Successfully committed {format(total.rows, big.mark = ',')} rows"))
    }
  } else {
    DBI::dbRollback(conn)
    log.df <- rbind(log.df, data.frame(
      operation = "rollback",
      chunk = NA,
      rows = 0,
      start_row = NA,
      end_row = NA,
      status = "failed",
      message = error.message,
      timestamp = Sys.time(),
      stringsAsFactors = FALSE
    ))

    if (verbose) {
      message(glue::glue("Transaction rolled back: {error.message}"))
    }
  }

  return(log.df)
}
