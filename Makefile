# ha_glacier.so is a MariaDB 13.0.2 storage engine. It is GPL because it
# includes MariaDB server headers. It does not live in the Glacier tree.
# MySQL will not load the resulting shared library.

-include Makefile.local

MARIADB_ROOT ?=
MARIADB_SRC ?=
CRYPT_INCLUDE ?=
GLACIER_INCLUDE ?= ../Glacier/include
GLACIER_LIBDIR ?= ../Glacier/zig-out/lib

ifeq ($(strip $(MARIADB_ROOT)),)
$(error Set MARIADB_ROOT to a MariaDB 13.0.2 prefix with include/mysql/server)
endif
ifeq ($(strip $(MARIADB_SRC)),)
$(error Set MARIADB_SRC to the MariaDB 13.0.2 source tree, for the wsrep headers)
endif

MARIADB_INC := $(MARIADB_ROOT)/include/mysql/server
MARIADB_PRIVATE := $(MARIADB_INC)/private

CXX ?= g++
CXXFLAGS ?= -O2 -fPIC -std=gnu++17 -fno-strict-aliasing -Wall -Wno-unused-parameter
CPPFLAGS += -DMYSQL_SERVER -DMYSQL_DYNAMIC_PLUGIN -DDBUG_OFF -DDISABLE_DTRACE
ifneq ($(wildcard $(CRYPT_INCLUDE)/crypt.h),)
CPPFLAGS += -I$(CRYPT_INCLUDE)
endif
CPPFLAGS += -I"$(MARIADB_PRIVATE)" \
	-I"$(MARIADB_INC)" \
	-I"$(MARIADB_SRC)/wsrep-lib/include" \
	-I"$(MARIADB_SRC)/wsrep-lib/wsrep-API/v26" \
	-I"$(GLACIER_INCLUDE)"

all: ha_glacier.so

ha_glacier.so: ha_glacier.cc ha_glacier.h
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -shared -o $@ ha_glacier.cc \
		-L"$(GLACIER_LIBDIR)" -lglacier \
		-Wl,-rpath,'$$ORIGIN/../Glacier/zig-out/lib'

clean:
	rm -f ha_glacier.so
