cmake_minimum_required(VERSION 3.12)
project(ha_glacier LANGUAGES CXX)

# Foundry configures this directory on its own and forwards CMAKE_PREFIX_PATH
# to a MariaDB that ships mariadb-plugin-config.cmake. One such build, one line.
find_package(mariadb-plugin REQUIRED)

set(GLACIER_INCLUDE "" CACHE PATH "Directory with glacier.h")
set(GLACIER_LIBDIR "" CACHE PATH "Directory with libglacier.so")
set(CRYPT_INCLUDE "" CACHE PATH "Directory with crypt.h, when the server headers need it")
set(MARIADB_EXTRA_INCLUDES "" CACHE STRING "More include directories, separated by semicolons")

find_path(GLACIER_HEADER glacier.h
  HINTS ${GLACIER_INCLUDE} ENV GLACIER_INCLUDE
  PATHS ${CMAKE_CURRENT_SOURCE_DIR}/../Glacier/include)
find_library(GLACIER_LIBRARY NAMES glacier
  HINTS ${GLACIER_LIBDIR} ENV GLACIER_LIBDIR
  PATHS ${CMAKE_CURRENT_SOURCE_DIR}/../Glacier/zig-out/lib)

if(NOT GLACIER_HEADER OR NOT GLACIER_LIBRARY)
  message(FATAL_ERROR "Point GLACIER_INCLUDE at glacier.h and GLACIER_LIBDIR at libglacier.so")
endif()

mariadb_add_plugin(glacier ha_glacier.cc
  STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES ${GLACIER_LIBRARY}
  VERSION "0.7.0"
  AUTHOR "Marcelo Tesla"
  DESCRIPTION "Read Parquet, Iceberg, and a Glacier warehouse")

if(NOT TARGET glacier)
  return()
endif()

get_filename_component(GLACIER_LIBDIR_REAL "${GLACIER_LIBRARY}" DIRECTORY)
target_include_directories(glacier PRIVATE ${GLACIER_HEADER})
if(CRYPT_INCLUDE)
  target_include_directories(glacier BEFORE PRIVATE ${CRYPT_INCLUDE})
endif()
if(MARIADB_EXTRA_INCLUDES)
  target_include_directories(glacier BEFORE PRIVATE ${MARIADB_EXTRA_INCLUDES})
endif()
target_compile_definitions(glacier PRIVATE MYSQL_SERVER DISABLE_DTRACE)
target_compile_options(glacier PRIVATE -fno-strict-aliasing)
target_compile_features(glacier PRIVATE cxx_std_17)
set_target_properties(glacier PROPERTIES
  BUILD_RPATH "${GLACIER_LIBDIR_REAL}"
  INSTALL_RPATH "$ORIGIN:${GLACIER_LIBDIR_REAL}")

include(CPack)
