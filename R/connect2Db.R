#' Connect to a Database
#'
#' Establishes a connection to a database using \pkg{DBI} with the provided
#' connection parameters. By default, uses an ODBC driver (\code{odbc::odbc()}).
#' Connection parameters must include driver, server, database, username, and password.
#'
#' @param db.info.ls A named list with database connection parameters. Must include:
#'   \itemize{
#'     \item \code{driver}: ODBC driver name
#'     \item \code{server}: Database server host
#'     \item \code{database}: Database name
#'     \item \code{username}: Database user name
#'     \item \code{password}: Database password
#'   }
#' @param drv A database driver object. Default is \code{odbc::odbc()}.
#'
#' @return A DBI connection object.
#' @export
#'
#' @importFrom DBI dbConnect
#' @importFrom odbc odbc
#'
#' @examples
#' \dontrun{
#' # Create connection info list
#' db.config.ls <- list(
#'   driver   = "SQL Server",
#'   server   = "localhost",
#'   database = "mydb",
#'   username = "user",
#'   password = "pass"
#' )
#'
#' # Connect using default ODBC driver
#' con <- connect2Db(db.info.ls = db.config.ls)
#'
#' # Connect using a specific driver (example: PostgreSQL)
#' con <- connect2Db(
#'   db.info.ls = db.config.ls,
#'   drv = RPostgreSQL::PostgreSQL()
#' )
#' }
connect2Db <- function(db.info.ls, drv = odbc::odbc()) {

  # Validate required parameters
  required.fields <- c("driver", "server", "database", "username", "password")
  missing.fields <- setdiff(required.fields, names(db.info.ls))

  if (length(missing.fields) > 0) {
    stop("Missing required fields in db.info.ls: ", paste(missing.fields, collapse = ", "))
  }

  # Create database connection
  con <- DBI::dbConnect(
    drv = drv,
    Driver = db.info.ls$driver,
    Server = db.info.ls$server,
    Database = db.info.ls$database,
    UID = db.info.ls$username,
    PWD = db.info.ls$password,
    TrustServerCertificate = "yes"
  )

  return(con)
}

