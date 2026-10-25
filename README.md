# ha_glacier

[Glacier](https://github.com/M-Tesla/Glacier) engine for MariaDB. GPL-2.0. Linking `libglacier.so` does not relicense that library.

It runs in MariaDB. `INSTALL SONAME 'ha_glacier.so'`.

## Build

Set `MARIADB_ROOT` to the server prefix, `MARIADB_SRC` to the matching source tree, and `CRYPT_INCLUDE` to the directory with `crypt.h`. `GLACIER_INCLUDE` and `GLACIER_LIBDIR` default to `../Glacier/include` and `../Glacier/zig-out/lib`.

```
make
```
