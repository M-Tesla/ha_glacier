# ha_glacier

[Glacier](https://github.com/M-Tesla/Glacier) engine for MariaDB. GPL-2.0. Linking `libglacier.so` does not relicense that library. This plugin is 0.7.0. It links `libglacier.so` from Glacier 0.7.0, `api_version` 2.

It runs in MariaDB 13.0.2. `INSTALL SONAME 'ha_glacier.so'`. MySQL does not load this `.so`.

```
CREATE TABLE orders (id BIGINT, note TEXT)
  ENGINE=Glacier COMMENT='/path/to/file.parquet';
SELECT SUM(id) FROM orders WHERE id > 1;
```

`COMMENT` is a Parquet file, an Iceberg table, or a warehouse. On a warehouse, the MariaDB database and table name are the Glacier name, so `sales.orders` reads `sales.orders`. A single-table `SELECT` with `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, and a simple `WHERE` runs as one Glacier statement. A join with another engine stays in MariaDB and scans the Glacier side. Writes are refused.

## Build

Set `MARIADB_ROOT` to the server prefix, `MARIADB_SRC` to the matching source tree, and `CRYPT_INCLUDE` to the directory with `crypt.h`. `GLACIER_INCLUDE` and `GLACIER_LIBDIR` default to `../Glacier/include` and `../Glacier/zig-out/lib`.

```
make
```
