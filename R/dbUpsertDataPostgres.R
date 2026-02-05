#' Upsert Data to PostgreSQL with ON CONFLICT and Chunking
#'
#' Efficiently upserts large data frames into a PostgreSQL table using
#' `INSERT ... ON CONFLICT`. If the target table does not exist, it is
#' created. Conflicts on key columns are resolved by comparing timestamps,
#' keeping either the earliest or latest record.
#'
#' @section Constraint Requirement:
#' PostgreSQL's `ON CONFLICT` clause requires a unique constraint or index on
#' the key columns. When this function creates a new table, it automatically
#' adds a unique constraint named `\{table_name\}_upsert_key`. If upserting to a
#' table created outside this function, you must ensure an appropriate unique
#' constraint or index exists on `key.cols`, otherwise the upsert will fail.
#'
#' @inheritParams dbUpsertDataMssql
#' @return A `data.frame` transaction log.
#' @export
#'
#' @importFrom DBI dbIsValid Id dbExistsTable dbGetQuery dbRemoveTable dbExecute
#' @importFrom glue glue
dbUpsertDataPostgres <- function(
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
    schema.name <- "public"
  }

  # Force timestamps into UTC
  if (inherits(new.data[[timestamp.col]], "POSIXt")) {
    attr(new.data[[timestamp.col]], "tzone") <- "UTC"
  }

  table.id <- DBI::Id(schema = schema.name, table = table.name)
  table.exists <- DBI::dbExistsTable(conn, table.id)
  table.str <- qualifyTablePg(schema.name, table.name)

  # If table doesn't exist, create it with initial data
  if (!table.exists) {
    dbInsertDataPostgres(
      conn = conn,
      new.data = new.data,
      schema.name = schema.name,
      table.name = table.name,
      chunk.size = chunk.size,
      overwrite = TRUE,
      verbose = verbose
    )

    # Create unique constraint on key columns for ON CONFLICT to work
    constraint.name <- paste0(table.name, "_upsert_key")
    key.cols.quoted <- paste(quoteIdentPg(key.cols), collapse = ", ")

    tryCatch({
      DBI::dbExecute(conn, glue::glue("
        ALTER TABLE {table.str}
        ADD CONSTRAINT {quoteIdentPg(constraint.name)}
        UNIQUE ({key.cols.quoted})
      "))
      if (verbose) message(glue::glue("Created unique constraint on ({paste(key.cols, collapse = ', ')})"))
    }, error = function(e) {
      if (verbose) message(glue::glue("Note: Could not create constraint (may already exist): {e$message}"))
    })

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

  } else {

    # Table exists - use staging table approach for large upserts
    temp.table.name <- sprintf("%s_staging_%s", table.name, format(Sys.time(), "%Y%m%d%H%M%S"))
    temp.table.id <- DBI::Id(schema = schema.name, table = temp.table.name)
    temp.table.str <- qualifyTablePg(schema.name, temp.table.name)

    # Create temp table as copy of target structure
    create.temp.sql <- glue::glue("
    CREATE TABLE {temp.table.str} (LIKE {table.str} INCLUDING ALL)
  ")

    tryCatch({
      DBI::dbExecute(conn, glue::glue("DROP TABLE IF EXISTS {temp.table.str}"))
      DBI::dbExecute(conn, create.temp.sql)
      if (verbose) message(glue::glue("Created staging table {temp.table.str}"))
    }, error = function(e) {
      stop(glue::glue("Failed to create staging table: {e$message}"))
    })

    # Insert new data into staging table
    dbInsertDataPostgres(
      conn = conn,
      new.data = new.data,
      schema.name = schema.name,
      table.name = temp.table.name,
      chunk.size = chunk.size,
      overwrite = TRUE,
      verbose = verbose
    )

    # Build the upsert SQL using INSERT ... ON CONFLICT
    non.key.cols <- setdiff(names(new.data), key.cols)
    col.list <- paste(quoteIdentPg(names(new.data)), collapse = ", ")
    key.cols.quoted <- paste(quoteIdentPg(key.cols), collapse = ", ")

    # Build UPDATE SET clause with timestamp condition
    timestamp.comparison <- if (keep == "last") ">" else "<"

    update.assignments <- paste(
      sprintf("%s = EXCLUDED.%s", quoteIdentPg(non.key.cols), quoteIdentPg(non.key.cols)),
      collapse = ", "
    )

    # PostgreSQL upsert with conditional update based on timestamp
    upsert.sql <- glue::glue("
    INSERT INTO {table.str} ({col.list})
    SELECT {col.list} FROM {temp.table.str}
    ON CONFLICT ({key.cols.quoted})
    DO UPDATE SET {update.assignments}
    WHERE EXCLUDED.{quoteIdentPg(timestamp.col)} {timestamp.comparison} {table.str}.{quoteIdentPg(timestamp.col)}
  ")

    # Execute upsert
    tryCatch({
      rows.affected <- DBI::dbExecute(conn, upsert.sql)

      if (verbose) {
        message(glue::glue("Upsert complete: {rows.affected} rows affected"))
      }

      log.df <- rbind(log.df, data.frame(
        operation = "upsert",
        chunk = NA,
        rows = nrow(new.data),
        start_row = 1,
        end_row = nrow(new.data),
        status = "success",
        message = glue::glue("rows_affected={rows.affected}"),
        timestamp = Sys.time()
      ))
    }, error = function(e) {
      log.df <<- rbind(log.df, data.frame(
        operation = "upsert",
        chunk = NA,
        rows = nrow(new.data),
        start_row = 1,
        end_row = nrow(new.data),
        status = "failed",
        message = e$message,
        timestamp = Sys.time()
      ))
      stop(glue::glue("Upsert failed: {e$message}"))
    })

    # Cleanup staging table
    if (DBI::dbExistsTable(conn, temp.table.id)) {
      DBI::dbRemoveTable(conn, temp.table.id)
      log.df <- rbind(log.df, data.frame(
        operation = "cleanup",
        chunk = NA,
        rows = 0,
        start_row = NA,
        end_row = NA,
        status = "success",
        message = "staging table removed",
        timestamp = Sys.time()
      ))
    }
  }
  return(log.df)
}
