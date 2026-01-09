#' Insert Spatial Data into SQL Database
#'
#' Inserts a spatial data frame (sf object) into a database table, converting
#' geometries to WKT text. Supports both SQL Server and PostgreSQL.
#'
#' @param conn A valid database connection object.
#' @param spatial.data An `sf` object to insert.
#' @param schema.name Schema name. Defaults based on dialect.
#' @param table.name Target table name.
#' @param geom.col Name for the geometry column in the database. Default is "geom_wkt".
#' @param chunk.size Number of rows per chunk. Default is `5000` (smaller due to WKT size).
#' @param overwrite Whether to overwrite existing data. Default is `FALSE`.
#' @param include.srid Whether to include SRID in WKT (as EWKT). Default is `TRUE`.
#' @param verbose Whether to print progress messages. Default is `TRUE`.
#' @param dialect Character string specifying database dialect: "mssql" or "postgres".
#'
#' @return A `data.frame` transaction log.
#' @export
#'
#' @importFrom sf st_as_text st_crs st_drop_geometry st_geometry
#' @importFrom glue glue
#'
#' @examples
#' \dontrun{
#' library(sf)
#' nc.sf <- st_read(system.file("shape/nc.shp", package = "sf"))
#' log.df <- dbInsertSpatial(
#'   conn = conn,
#'   spatial.data = nc.sf,
#'   table.name = "nc_counties",
#'   dialect = "postgres"
#' )
#' }
dbInsertSpatial <- function(
    conn,
    spatial.data,
    schema.name = NULL,
    table.name,
    geom.col = "geom_wkt",
    chunk.size = 5000L,
    overwrite = FALSE,
    include.srid = TRUE,
    verbose = TRUE,
    dialect
) {
  if (!is.character(dialect) || length(dialect) != 1) {
    stop("Argument 'dialect' must be a character string")
  }
  if (!dialect %in% c("mssql", "postgres")) {
    stop("Argument 'dialect' must be 'mssql' or 'postgres', got: '", dialect, "'")
  }
  if (!inherits(spatial.data, "sf")) {
    stop("Argument 'spatial.data' must be an sf object")
  }

  # Extract CRS info
  crs.obj <- sf::st_crs(spatial.data)
  srid <- if (!is.na(crs.obj$epsg)) crs.obj$epsg else NA

  if (verbose) {
    srid.msg <- ifelse(is.na(srid), "unknown", srid)
    message(glue::glue(
      "Converting {nrow(spatial.data)} features to WKT (SRID: {srid.msg})"
    ))
  }

  # Convert geometry to WKT
  wkt.vec <- sf::st_as_text(sf::st_geometry(spatial.data))

  # Optionally prepend SRID for EWKT format
  if (include.srid && !is.na(srid)) {
    wkt.vec <- paste0("SRID=", srid, ";", wkt.vec)
  }

  # Create regular data frame with WKT column
  output.df <- sf::st_drop_geometry(spatial.data)
  output.df[[geom.col]] <- wkt.vec

  # Use existing insert function
  log.df <- dbInsertData(
    conn = conn,
    new.data = output.df,
    schema.name = schema.name,
    table.name = table.name,
    chunk.size = chunk.size,
    overwrite = overwrite,
    verbose = verbose,
    dialect = dialect
  )

  return(log.df)
}
