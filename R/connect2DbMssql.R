#' Connect to SQL Server Database
#'
#' Internal function to establish SQL Server connection via ODBC.
#'
#' @param db.info.ls A named list with: driver, server, database, username, password,
#'   and optionally port and trust.server.cert.
#' @param trust.cert Character string for SQL Server SSL certificate validation.
#'   "Yes" to trust self-signed certificates, "No" to enforce validation.
#'
#' @return A DBI connection object.
#'
#' @importFrom DBI dbConnect
#' @importFrom odbc odbc
#'
#' @keywords internal
connect2DbMssql <- function(db.info.ls, trust.cert = "Yes") {
  required.fields <- c("driver", "server", "database", "username", "password")
  missing.fields <- setdiff(required.fields, names(db.info.ls))

  if (length(missing.fields) > 0) {
    stop(
      "Missing required fields for mssql connection: ",
      paste(missing.fields, collapse = ", ")
    )
  }

  # SQL Server ODBC expects port appended to server with comma separator
  server.str <- db.info.ls$server
  if (!is.null(db.info.ls$port)) {
    server.str <- paste0(db.info.ls$server, ",", db.info.ls$port)
  }

  conn <- DBI::dbConnect(
    drv = odbc::odbc(),
    Driver = db.info.ls$driver,
    Server = server.str,
    Database = db.info.ls$database,
    UID = db.info.ls$username,
    PWD = db.info.ls$password,
    TrustServerCertificate = trust.cert
  )

  return(conn)
}
