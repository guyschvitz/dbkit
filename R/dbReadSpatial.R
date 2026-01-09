#' Read Spatial Data from SQL Database
#'
#' Reads a table containing WKT geometries and converts back to an sf object.
#'
#' @param conn A valid database connection object.
#' @param schema.name Schema name. Defaults based on dialect.
#' @param table.name Table name.
#' @param geom.col Name of the WKT geometry column. Default is "geom_wkt".
#' @param default.srid SRID to use if not embedded in WKT. Default is `4326`.
#' @param dialect Character string specifying database dialect: "mssql" or "postgres".
#'
#' @return An `sf` object.
#' @export
#'
#' @importFrom sf st_as_sfc st_sf
#' @importFrom DBI dbGetQuery
#' @importFrom glue glue
#'
#' @examples
#' \dontrun{
#' nc.sf <- dbReadSpatial(
#'   conn = conn,
#'   table.name = "nc_counties",
#'   dialect = "postgres"
#' )
#' }
dbReadSpatial <- function(
    conn,
    schema.name = NULL,
    table.name,
    geom.col = "geom_wkt",
    default.srid = 4326L,
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

  # Build qualified table name
  table.str <- switch(dialect,
                      mssql = qualifyTableMs(schema.name, table.name),
                      postgres = qualifyTablePg(schema.name, table.name)
  )

  query.df <- DBI::dbGetQuery(conn, glue::glue("SELECT * FROM {table.str}"))

  if (!geom.col %in% names(query.df)) {
    stop("Geometry column '", geom.col, "' not found in table")
  }

  wkt.vec <- query.df[[geom.col]]

  # Parse SRID from EWKT if present (format: "SRID=4326;POLYGON(...)")
  srid <- default.srid
  if (length(wkt.vec) > 0 && grepl("^SRID=", wkt.vec[1])) {
    srid <- as.integer(sub("^SRID=([0-9]+);.*", "\\1", wkt.vec[1]))
    wkt.vec <- sub("^SRID=[0-9]+;", "", wkt.vec)
  }

  # Convert WKT to geometry
  geom.sfc <- sf::st_as_sfc(wkt.vec, crs = srid)

  # Remove WKT column and create sf object
  query.df[[geom.col]] <- NULL
  result.sf <- sf::st_sf(query.df, geometry = geom.sfc)

  return(result.sf)
}
