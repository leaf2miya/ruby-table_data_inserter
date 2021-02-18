# Random Data Inserter
  Easily insert random data into the database table
## Description
  Read table information(schema, foreign keys...), generate and insert matching random data.
  Target table are all tables in the specified database.

  Warning: deletes the table data of the specified database when running this tool.
           don't revert it.
## Requirement
### Operating systems
  * CentOS (8)
### Middleware
  * MySQL (8 and later)
  * PostgreSQL (9.2 and later)
### Software
  * Ruby 2.7 and later is required
## Usage
 ```
  Usage: random_inserter.rb connect_uri count
      -v                               put verbose message
          --ignore TBL                 ignore insert table
 ```

 * Argument "count" is insert max record count per one table.
   If you specify 100, insert number of records between 0 and 100.
   (because skip the insertion of unique constraint records)
## Instration
 1. git clone
 1. bundle install (without development)
## Examples
  * for MySQL(insert max 1,000 records per table)
  ```
    ruby random_inserter.rb mysql2://user:password@hostname:port/db_name 1000
  ```
  * for PostgreSQL(insert max 1,000 records per table)(skip "hoge" and "fuga" table)
  ```
    ruby random_inserter.rb --ignore=hoge --ignore=fuga postgres://user:password@hostname:port/db_name 1000
  ```
## Licence
 MIT License
