#' Connect to a Database
#'
#' Establishes a connection to a database using \pkg{DBI} with the provided
#' connection parameters. Supports both SQL Server (via ODBC) and PostgreSQL
#' (via RPostgres).
#'
#' @param db.info.ls A named list with database connection parameters. Required
#'   fields depend on dialect:
#'   \describe{
#'     \item{For mssql:}{
#'       \itemize{
#'         \item \code{driver}: ODBC driver name (e.g., "ODBC Driver 17 for SQL Server")
#'         \item \code{server}: Database server host
#'         \item \code{database}: Database name
#'         \item \code{username}: Database user name
#'         \item \code{password}: Database password
#'         \item \code{port}: (Optional) Port number, defaults to 1433
#'       }
#'     }
#'     \item{For postgres:}{
#'       \itemize{
#'         \item \code{host}: Database server host
#'         \item \code{dbname}: Database name
#'         \item \code{user}: Database user name
#'         \item \code{password}: Database password
#'         \item \code{port}: (Optional) Port number, defaults to 5432
#'       }
#'     }
#'   }
#' @param dialect Database dialect: "mssql" or "postgres".
#'
#' @return A DBI connection object.
#' @export
#'
#' @importFrom DBI dbConnect
#'
#' @examples
#' \dontrun{
#' # SQL Server connection
#' mssql.config.ls <- list(
#'   driver   = "ODBC Driver 17 for SQL Server",
#'   server   = "localhost",
#'   database = "mydb",
#'   username = "user",
#'   password = "pass"
#' )
#' conn <- connect2Db(
#'   db.info.ls = mssql.config.ls,
#'   dialect = "mssql"
#' )
#'
#' # PostgreSQL connection
#' pg.config.ls <- list(
#'   host     = "localhost",
#'   dbname   = "mydb",
#'   user     = "user",
#'   password = "pass",
#'   port     = 5432
#' )
#' conn <- connect2Db(
#'   db.info.ls = pg.config.ls,
#'   dialect = "postgres"
#' )
#' }
connect2Db <- function(db.info.ls, dialect) {
  if (!is.character(dialect) || length(dialect) != 1) {
    stop("Argument 'dialect' must be a character string")
  }
  if (!dialect %in% c("mssql", "postgres")) {
    stop("Argument 'dialect' must be 'mssql' or 'postgres', got: '", dialect, "'")
  }
  if (!is.list(db.info.ls)) {
    stop("Argument 'db.info.ls' must be a named list")
  }

  conn <- switch(dialect,
                 mssql = connect2DbMssql(db.info.ls = db.info.ls),
                 postgres = connect2DbPostgres(db.info.ls = db.info.ls)
  )

  return(conn)
}
