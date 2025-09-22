# dbkit

Utilities for connecting to Microsoft SQL Server and performing efficient data operations.\
Includes helpers for establishing database connections, inserting data in chunks with transaction support, and performing memory-efficient upserts using SQL Server `MERGE`.

## Installation

You can install the development version from source with:

``` r
# install.packages("devtools")
devtools::install_github("yourusername/dbkit")
```

## Example

``` r
library(dbkit)

# Connection info
db.config <- list(
  driver   = "SQL Server",
  server   = "localhost",
  database = "mydb",
  username = "user",
  password = "pass"
)

# Connect
con <- connect2Db(db.info.ls = db.config)

# Insert data in chunks
log.insert <- dbInsertDataSqlServer(
  conn = con,
  data.df = data.frame(id = 1:100, value = letters[1:100]),
  table.id = DBI::Id(schema = "dbo", table = "my_table"),
  chunk.size = 20
)

# Upsert data
log.upsert <- dbUpsertDataSqlServer(
  conn = con,
  new.data = data.frame(id = 1:5, value = LETTERS[1:5], updated_at = Sys.time()),
  schema.name = "dbo",
  table.name = "my_table",
  key.cols = "id",
  timestamp.col = "updated_at"
)
```
