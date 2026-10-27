# ha_glacier

[Glacier](https://github.com/M-Tesla/Glacier) engine for MariaDB. GPL-2.0. Linking `libglacier.so` does not relicense that library. This plugin is 0.7.0. It links `libglacier.so` from Glacier 0.7.0, `api_version` 2.

`INSTALL SONAME 'ha_glacier.so'`. MySQL does not load this `.so`. Each MariaDB line needs its own build of this plugin. A `.so` from one line does not load on another.

```
CREATE TABLE orders (id BIGINT, note TEXT)
  ENGINE=Glacier CONNECTION='/path/to/file.parquet';
SELECT SUM(id) FROM orders WHERE id > 1;
```

`CONNECTION` is a Parquet file, an Iceberg table, or a warehouse. On MariaDB 11.4 and 11.8 that clause is the table connect string. From 12.3 it is an engine option. The statement above is the same either way. `COMMENT` stays a comment. On a warehouse, the MariaDB database and table name are the Glacier name, so `sales.orders` reads `sales.orders`. A single-table `SELECT` with `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, and a simple `WHERE` runs as one Glacier statement. A join with another engine stays in MariaDB and scans the Glacier side. Writes are refused.

## Build

`CMakeLists.txt` is what Foundry adds, one package per line. To compile one line by hand, set `MARIADB_ROOT` to that server prefix, `MARIADB_SRC` to the matching source tree, and `CRYPT_INCLUDE` to the directory with `crypt.h`. `GLACIER_INCLUDE` and `GLACIER_LIBDIR` default to `../Glacier/include` and `../Glacier/zig-out/lib`.

```
make
```
