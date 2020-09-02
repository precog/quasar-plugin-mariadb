# Quasar MariaDB/MySQL Plugins [![Discord](https://img.shields.io/discord/373302030460125185.svg?logo=discord)](https://discord.gg/pSSqJrr)

## Datasource

The MariaDB/MySQL datasource plugin enables Quasar to load data from MariaDB, MySQL or other protocol-compliant stores. Most native column types are supported with the notable exception of `BINARY`/`BLOB` variants and all `GEOMETRY` types.

### Datasource Configuration

JSON configuration required to construct a MariaDB datasource.

```
{
  "connection": <connection-configuration>
}
```

* `connection`: A [connection configuration](#connection-configuration) object.

## Destination

The MariaDB/MySQL destination plugin enables Quasar to load data into MariaDB, MySQL or other protocol-compliant stores. Loading is done via the `LOAD DATA LOCAL INFILE` statement to achieve good performance.

Please ensure the destination server has `LOCAL INFILE` support enabled.

### Destination Configuration

JSON configuration required to construct a MariaDB destination.

```
{
  "connection": <connection-configuration>,
  "writeMode": "create" | "replace" | "truncate" | "append"
}
```

* `connection`: A [connection configuration](#connection-configuration) object.
* `writeMode`: Indicates how to handle loading data into an existing table
  * `create`: prevent loading data into an existing table, erroring if it exists
  * `replace`: `DROP` and recreate an existing table prior to loading data
  * `truncate`: `TRUNCATE` an existing table prior to loading data
  * `append`: appends to an existing table, creating it if it doesn't exist

## Connection Configuration

JSON configurating describing how to connect to MariaDB.

```
{
  "jdbcUrl": String
  [, "maxConcurrency": Number]
  [, "maxLifetimeSecs": Number]
}
```

* `jdbcUrl`: a MariaDB [connection string](https://mariadb.com/kb/en/about-mariadb-connector-j/#connection-strings). Note that any connection parameter values containing URI [reserved characters](https://tools.ietf.org/html/rfc3986#section-2.2) must be [percent encoded](https://tools.ietf.org/html/rfc3986#section-2.1) to avoid ambiguity.
* `maxConcurrency` (optional): the maximum number of simultaneous connections to the database (default: 8)
* `maxLifetimeSecs` (optional): the maximum lifetime, in seconds, of idle connections. If your database or infrastructure imposes any limit on idle connections, make sure to set this value to at most a few seconds less than the limit (default: 300 seconds)
