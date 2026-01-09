#' Connect to SQL Server Database
#'
#' Internal function to establish SQL Server connection via ODBC.
#'
#' @param db.info.ls A named list with: driver, server, database, username, password,
#'   and optionally port.
#'
#' @return A DBI connection object.
#'
#' @importFrom DBI dbConnect
#' @importFrom odbc odbc
#'
#' @keywords internal
connect2DbMssql <- function(db.info.ls) {
  required.fields <- c("driver", "server", "database", "username", "password")
  missing.fields <- setdiff(required.fields, names(db.info.ls))

  if (length(missing.fields) > 0) {
    stop(
      "Missing required fields for mssql connection: ",
      paste(missing.fields, collapse = ", ")
    )
  }

  port <- if (!is.null(db.info.ls$port)) db.info.ls$port else 1433L

  conn <- DBI::dbConnect(
    drv = odbc::odbc(),
    Driver = db.info.ls$driver,
    Server = db.info.ls$server,
    Database = db.info.ls$database,
    UID = db.info.ls$username,
    PWD = db.info.ls$password,
    Port = port
  )

  return(conn)
}
