#' Connect to PostgreSQL Database
#'
#' Internal function to establish PostgreSQL connection via RPostgres.
#'
#' @param db.info.ls A named list with: host, dbname, user, password,
#'   and optionally port.
#'
#' @return A DBI connection object.
#'
#' @importFrom DBI dbConnect
#' @importFrom RPostgres Postgres
#'
#' @keywords internal
connect2DbPostgres <- function(db.info.ls) {
  required.fields <- c("host", "dbname", "user", "password")
  missing.fields <- setdiff(required.fields, names(db.info.ls))

  if (length(missing.fields) > 0) {
    stop(
      "Missing required fields for postgres connection: ",
      paste(missing.fields, collapse = ", ")
    )
  }

  port <- if (!is.null(db.info.ls$port)) db.info.ls$port else 5432L

  conn <- DBI::dbConnect(
    drv = RPostgres::Postgres(),
    host = db.info.ls$host,
    dbname = db.info.ls$dbname,
    user = db.info.ls$user,
    password = db.info.ls$password,
    port = port
  )

  return(conn)
}
