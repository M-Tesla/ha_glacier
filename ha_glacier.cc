/* Copyright (c) 2026 Marcelo Tesla

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1335 USA */

#include "my_global.h"
#include "mysql/plugin.h"
#include "ha_glacier.h"
#include "sql_class.h"
#include "unireg.h"

/*
 * MYSQL_DYNAMIC_PLUGIN turns my_printf_error into a service pointer the
 * server does not export. The function itself is exported, so call that.
 */
#undef my_printf_error
extern "C" void my_printf_error(unsigned int my_err, const char *format,
                                unsigned long MyFlags, ...)
    __attribute__((format(printf, 2, 4)));

#include <cstdio>
#include <cstring>
#include <ctime>
#include <new>
#include <string>

static const char *ha_glacier_exts[] = {NullS};

static handler *glacier_create_handler(handlerton *hton, TABLE_SHARE *table,
                                       MEM_ROOT *mem_root);

#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key glacier_share_mutex_key;
static PSI_mutex_info glacier_mutexes[] = {
    {&glacier_share_mutex_key, "Glacier_share::mutex", 0}};
static void
init_glacier_psi(void)
{
  mysql_mutex_register("glacier", glacier_mutexes, 1);
}
#else
static PSI_mutex_key glacier_share_mutex_key;
static void
init_glacier_psi(void)
{
}
#endif

Glacier_share::Glacier_share()
{
  thr_lock_init(&lock);
  mysql_mutex_init(glacier_share_mutex_key, &mutex, MY_MUTEX_INIT_FAST);
}

static int
glacier_plugin_init(void *p)
{
  handlerton *hton = (handlerton *) p;

  init_glacier_psi();
  hton->create = glacier_create_handler;
  hton->flags = HTON_CAN_RECREATE;
  hton->tablefile_extensions = ha_glacier_exts;
  return 0;
}

static handler *
glacier_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root)
{
  return new (mem_root) ha_glacier(hton, table);
}

ha_glacier::ha_glacier(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg), share_(NULL), path_(NULL), db_(NULL), conn_(NULL),
      result_(NULL), batch_pos_(0), batch_len_(0), current_row_(UINT64_MAX)
{
  ref_length = sizeof(uint64_t);
  memset(&array_, 0, sizeof(array_));
  memset(&schema_, 0, sizeof(schema_));
}

ha_glacier::~ha_glacier()
{
  release_scan();
  free(path_);
  path_ = NULL;
}

Glacier_share *
ha_glacier::get_share()
{
  Glacier_share *tmp;

  lock_shared_ha_data();
  if (!(tmp = static_cast<Glacier_share *>(get_ha_share_ptr())))
  {
    tmp = new (std::nothrow) Glacier_share;
    if (tmp)
      set_ha_share_ptr(static_cast<Handler_share *>(tmp));
  }
  unlock_shared_ha_data();
  return tmp;
}

static void
trim_inplace(char *s)
{
  char *start = s;
  size_t n;

  while (*start == ' ' || *start == '\t' || *start == '\n' || *start == '\r')
    start++;
  if (start != s)
    memmove(s, start, strlen(start) + 1);
  n = strlen(s);
  while (n > 0 && (s[n - 1] == ' ' || s[n - 1] == '\t' || s[n - 1] == '\n' ||
                   s[n - 1] == '\r'))
  {
    s[--n] = '\0';
  }
}

static bool
sql_ident(const char *s, size_t n)
{
  size_t i;

  if (n == 0 || n > 256 || s == NULL)
    return false;
  if (!((s[0] >= 'A' && s[0] <= 'Z') || (s[0] >= 'a' && s[0] <= 'z') || s[0] == '_'))
    return false;
  for (i = 1; i < n; i++)
  {
    char c = s[i];
    if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
          c == '_'))
      return false;
  }
  return true;
}

int
ha_glacier::fail(const char *msg)
{
  my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0),
                  (msg != NULL && msg[0] != '\0') ? msg : "failed");
  return HA_ERR_GENERIC;
}

int
ha_glacier::fail_free(char *err)
{
  std::string msg = (err != NULL && err[0] != '\0') ? err : "failed";

  glacier_free(err);
  return fail(msg.c_str());
}

void
ha_glacier::release_batch()
{
  if (array_.release)
    array_.release(&array_);
  if (schema_.release)
    schema_.release(&schema_);
  memset(&array_, 0, sizeof(array_));
  memset(&schema_, 0, sizeof(schema_));
  batch_pos_ = 0;
  batch_len_ = 0;
}

void
ha_glacier::release_scan()
{
  release_batch();
  if (result_)
    glacier_result_destroy(result_);
  result_ = NULL;
  if (conn_)
    glacier_disconnect(conn_);
  conn_ = NULL;
  if (db_)
    glacier_close(db_);
  db_ = NULL;
  current_row_ = UINT64_MAX;
}

int
ha_glacier::open(const char *name, int mode, uint test_if_locked)
{
  const LEX_CSTRING comment = table->s->comment;

  (void) name;
  (void) mode;
  (void) test_if_locked;
  if (!(share_ = get_share()))
    return HA_ERR_OUT_OF_MEM;
  thr_lock_data_init(&share_->lock, &lock_, NULL);
  if (comment.str == NULL || comment.length == 0 || comment.length > 16384)
    return fail("table needs COMMENT='path' of a Parquet file, an Iceberg table, or a warehouse");
  free(path_);
  path_ = (char *) malloc(comment.length + 1);
  if (path_ == NULL)
    return HA_ERR_OUT_OF_MEM;
  memcpy(path_, comment.str, comment.length);
  path_[comment.length] = '\0';
  trim_inplace(path_);
  if (path_[0] == '\0')
    return fail("table needs COMMENT='path' of a Parquet file, an Iceberg table, or a warehouse");
  return 0;
}

int
ha_glacier::close(void)
{
  release_scan();
  free(path_);
  path_ = NULL;
  return 0;
}

static int
glacier_read_only(void)
{
  my_printf_error(ER_UNKNOWN_ERROR, "Glacier tables are read only", MYF(0));
  return HA_ERR_WRONG_COMMAND;
}

int
ha_glacier::write_row(const uchar *)
{
  return glacier_read_only();
}

int
ha_glacier::update_row(const uchar *, const uchar *)
{
  return glacier_read_only();
}

int
ha_glacier::delete_row(const uchar *)
{
  return glacier_read_only();
}

int
ha_glacier::delete_all_rows(void)
{
  return glacier_read_only();
}

int
ha_glacier::external_lock(THD *, int)
{
  return 0;
}

int
ha_glacier::info(uint)
{
  if (stats.records < 2)
    stats.records = 2;
  return 0;
}

int
ha_glacier::create(const char *name, TABLE *, HA_CREATE_INFO *create_info)
{
  (void) name;
  if (create_info->comment.str == NULL || create_info->comment.length == 0)
    return fail("CREATE TABLE needs COMMENT='path' of a Parquet file, an Iceberg table, or a warehouse");
  return 0;
}

THR_LOCK_DATA **
ha_glacier::store_lock(THD *, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock_.type == TL_UNLOCK)
    lock_.type = lock_type;
  *to++ = &lock_;
  return to;
}

int
ha_glacier::open_query()
{
  std::string sql = "SELECT ";
  bool first = true;
  char *err = NULL;

  release_scan();
  if (path_ == NULL || path_[0] == '\0')
    return fail("table has no COMMENT path");
  if (glacier_api_version() != GLACIER_API_VERSION)
    return fail("libglacier API does not match this plugin");
  if (table->field == NULL)
    return fail("table has no columns");
  for (Field **fp = table->field; *fp; fp++)
  {
    Field *field = *fp;
    if (!sql_ident(field->field_name.str, field->field_name.length))
      return fail("column names must be letters, digits, and underscores");
    if (!first)
      sql += ", ";
    sql.append(field->field_name.str, field->field_name.length);
    first = false;
  }
  if (first)
    return fail("table has no columns");

  db_ = glacier_open(path_, &err);
  if (db_ == NULL)
    return fail_free(err);
  conn_ = glacier_connect(db_, &err);
  if (conn_ == NULL)
    return fail_free(err);
  result_ = glacier_query(conn_, sql.c_str(), &err);
  if (result_ == NULL)
    return fail_free(err);
  if (glacier_result_error(result_) != NULL)
  {
    std::string msg = glacier_result_error(result_);
    release_scan();
    return fail(msg.c_str());
  }
  current_row_ = UINT64_MAX;
  return 0;
}

int
ha_glacier::rnd_init(bool scan)
{
  (void) scan;
  return open_query();
}

int
ha_glacier::rnd_end()
{
  release_scan();
  return 0;
}

static bool
arrow_is_null(const struct ArrowArray *col, int64_t row)
{
  const uint8_t *bits;
  int64_t i;

  if (col->buffers == NULL || col->n_buffers < 1 || col->buffers[0] == NULL)
    return false;
  if (col->null_count == 0)
    return false;
  bits = (const uint8_t *) col->buffers[0];
  i = col->offset + row;
  return ((bits[i >> 3] >> (i & 7)) & 1) == 0;
}

static const void *
arrow_buf(const struct ArrowArray *col, int64_t which)
{
  if (col->buffers == NULL || col->n_buffers <= which || col->buffers[which] == NULL)
    return NULL;
  return col->buffers[which];
}

static int
i128_text(const uint8_t *le, int scale, char *out, size_t cap)
{
  unsigned __int128 mag;
  int neg;
  int nd = 0;
  int pos = 0;
  int i;
  char digs[48];

  if (cap < 4)
    return -1;
  memcpy(&mag, le, 16);
  neg = (le[15] & 0x80) != 0;
  if (neg)
    mag = ~mag + 1;
  if (mag == 0)
    digs[nd++] = '0';
  while (mag != 0 && nd < (int) sizeof(digs))
  {
    digs[nd++] = (char) ('0' + (int) (mag % 10));
    mag /= 10;
  }
  if (mag != 0)
    return -1;
  if (neg && !(nd == 1 && digs[0] == '0'))
    out[pos++] = '-';
  if (scale <= 0)
  {
    for (i = nd - 1; i >= 0; i--)
    {
      if (pos + 1 >= (int) cap)
        return -1;
      out[pos++] = digs[i];
    }
    for (i = 0; i < -scale; i++)
    {
      if (pos + 1 >= (int) cap)
        return -1;
      out[pos++] = '0';
    }
    out[pos] = '\0';
    return pos;
  }
  if (nd > scale)
  {
    for (i = nd - 1; i >= scale; i--)
    {
      if (pos + 1 >= (int) cap)
        return -1;
      out[pos++] = digs[i];
    }
  }
  else
  {
    if (pos + 1 >= (int) cap)
      return -1;
    out[pos++] = '0';
  }
  if (pos + 1 >= (int) cap)
    return -1;
  out[pos++] = '.';
  for (i = scale - 1; i >= 0; i--)
  {
    if (pos + 1 >= (int) cap)
      return -1;
    out[pos++] = (i < nd) ? digs[i] : '0';
  }
  out[pos] = '\0';
  return pos;
}

static bool
field_is_int(enum_field_types t)
{
  switch (t)
  {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_YEAR:
      return true;
    default:
      return false;
  }
}

static bool
timestamp_us(const char *fmt, int64_t raw, int64_t *us)
{
  if (fmt[0] != 't' || fmt[1] != 's' || fmt[2] == '\0')
    return false;
  switch (fmt[2])
  {
    case 's':
      *us = raw * 1000000LL;
      return true;
    case 'm':
      *us = raw * 1000LL;
      return true;
    case 'u':
      *us = raw;
      return true;
    case 'n':
      *us = raw / 1000LL;
      return true;
    default:
      return false;
  }
}

static int
store_text(Field *field, const char *data, size_t len)
{
  return field->store(data, len, &my_charset_utf8mb4_general_ci);
}

int
ha_glacier::write_record(int64_t batch_row)
{
  MY_BITMAP *old_map;
  uint i;

  if (schema_.children == NULL || array_.children == NULL)
    return fail("Arrow batch is missing its columns");
  if (schema_.n_children != (int64_t) table->s->fields)
    return fail("Glacier returned a different number of columns than the table");

  empty_record(table);
  old_map = tmp_use_all_columns(table, &table->write_set);
  for (i = 0; i < table->s->fields; i++)
  {
    Field *field = table->field[i];
    const struct ArrowSchema *fs = schema_.children[i];
    const struct ArrowArray *col = array_.children[i];
    const char *fmt;
    int64_t ix;
    int rc = 0;

    if (fs == NULL || col == NULL || fs->format == NULL)
    {
      tmp_restore_column_map(&table->write_set, old_map);
      return fail("Arrow batch is missing a column");
    }
    if (fs->dictionary != NULL || col->dictionary != NULL || fs->n_children != 0)
    {
      tmp_restore_column_map(&table->write_set, old_map);
      return fail("nested or dictionary-encoded values are not supported");
    }
    if (arrow_is_null(&array_, batch_row) || arrow_is_null(col, batch_row))
    {
      field->set_null();
      continue;
    }
    field->set_notnull();
    fmt = fs->format;
    ix = col->offset + batch_row;
    if (strcmp(fmt, "b") == 0)
    {
      const uint8_t *bits = (const uint8_t *) arrow_buf(col, 1);
      int v;
      if (bits == NULL)
      {
        tmp_restore_column_map(&table->write_set, old_map);
        return fail("boolean column is missing its value buffer");
      }
      v = (bits[ix >> 3] >> (ix & 7)) & 1;
      rc = field->store((longlong) v, false);
    }
    else if (strcmp(fmt, "i") == 0)
    {
      const int32_t *v = (const int32_t *) arrow_buf(col, 1);
      if (v == NULL)
      {
        tmp_restore_column_map(&table->write_set, old_map);
        return fail("int32 column is missing its value buffer");
      }
      rc = field->store((longlong) v[ix], (field->flags & UNSIGNED_FLAG) != 0);
    }
    else if (strcmp(fmt, "l") == 0)
    {
      const int64_t *v = (const int64_t *) arrow_buf(col, 1);
      if (v == NULL)
      {
        tmp_restore_column_map(&table->write_set, old_map);
        return fail("int64 column is missing its value buffer");
      }
      rc = field->store((longlong) v[ix], (field->flags & UNSIGNED_FLAG) != 0);
    }
    else if (strcmp(fmt, "f") == 0 || strcmp(fmt, "g") == 0)
    {
      double d;
      if (strcmp(fmt, "f") == 0)
      {
        const float *v = (const float *) arrow_buf(col, 1);
        if (v == NULL)
        {
          tmp_restore_column_map(&table->write_set, old_map);
          return fail("float column is missing its value buffer");
        }
        d = (double) v[ix];
      }
      else
      {
        const double *v = (const double *) arrow_buf(col, 1);
        if (v == NULL)
        {
          tmp_restore_column_map(&table->write_set, old_map);
          return fail("double column is missing its value buffer");
        }
        d = v[ix];
      }
      rc = field->store(d);
    }
    else if (strcmp(fmt, "u") == 0 || strcmp(fmt, "U") == 0)
    {
      const char *bytes = (const char *) arrow_buf(col, 2);
      int64_t start;
      int64_t end;
      if (strcmp(fmt, "u") == 0)
      {
        const int32_t *off = (const int32_t *) arrow_buf(col, 1);
        if (off == NULL)
        {
          tmp_restore_column_map(&table->write_set, old_map);
          return fail("string column is missing its offset buffer");
        }
        start = off[ix];
        end = off[ix + 1];
      }
      else
      {
        const int64_t *off = (const int64_t *) arrow_buf(col, 1);
        if (off == NULL)
        {
          tmp_restore_column_map(&table->write_set, old_map);
          return fail("string column is missing its offset buffer");
        }
        start = off[ix];
        end = off[ix + 1];
      }
      if (end < start)
      {
        tmp_restore_column_map(&table->write_set, old_map);
        return fail("string column has a bad offset");
      }
      if (end == start)
        rc = store_text(field, "", 0);
      else if (bytes == NULL)
      {
        tmp_restore_column_map(&table->write_set, old_map);
        return fail("string column is missing its byte buffer");
      }
      else
        rc = store_text(field, bytes + start, (size_t) (end - start));
    }
    else if (strncmp(fmt, "ts", 2) == 0)
    {
      const int64_t *v = (const int64_t *) arrow_buf(col, 1);
      int64_t us;
      if (v == NULL || !timestamp_us(fmt, v[ix], &us))
      {
        tmp_restore_column_map(&table->write_set, old_map);
        return fail("timestamp column is not supported");
      }
      if (field_is_int(field->type()))
        rc = field->store((longlong) us, false);
      else
      {
        time_t sec = (time_t) (us / 1000000LL);
        int usec = (int) (us % 1000000LL);
        struct tm tm;
        char textv[40];
        int n;
        if (usec < 0)
        {
          sec -= 1;
          usec += 1000000;
        }
        if (gmtime_r(&sec, &tm) == NULL)
        {
          tmp_restore_column_map(&table->write_set, old_map);
          return fail("timestamp is out of range");
        }
        n = snprintf(textv, sizeof(textv), "%04d-%02d-%02d %02d:%02d:%02d.%06d",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                     tm.tm_sec, usec);
        rc = store_text(field, textv, (size_t) n);
      }
    }
    else if (strncmp(fmt, "d:", 2) == 0)
    {
      const uint8_t *raw = (const uint8_t *) arrow_buf(col, 1);
      int scale = 0;
      char textv[80];
      int n;
      if (raw == NULL)
      {
        tmp_restore_column_map(&table->write_set, old_map);
        return fail("decimal column is missing its value buffer");
      }
      sscanf(fmt + 2, "%*d,%d", &scale);
      n = i128_text(raw + (size_t) ix * 16, scale, textv, sizeof(textv));
      if (n < 0)
      {
        tmp_restore_column_map(&table->write_set, old_map);
        return fail("decimal value does not fit");
      }
      rc = store_text(field, textv, (size_t) n);
    }
    else if (strncmp(fmt, "w:16", 4) == 0)
    {
      const uint8_t *raw = (const uint8_t *) arrow_buf(col, 1);
      const uint8_t *b;
      char hex[37];
      int n;
      if (raw == NULL)
      {
        tmp_restore_column_map(&table->write_set, old_map);
        return fail("fixed-width column is missing its value buffer");
      }
      b = raw + (size_t) ix * 16;
      n = snprintf(hex, sizeof(hex),
                   "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x", b[0],
                   b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12],
                   b[13], b[14], b[15]);
      rc = store_text(field, hex, (size_t) n);
    }
    else
    {
      char textv[80];
      tmp_restore_column_map(&table->write_set, old_map);
      snprintf(textv, sizeof(textv), "Arrow format \"%s\" is not supported", fmt);
      return fail(textv);
    }
    if (rc < 0)
    {
      tmp_restore_column_map(&table->write_set, old_map);
      return fail("writing a column into the MariaDB row failed");
    }
  }
  tmp_restore_column_map(&table->write_set, old_map);
  return 0;
}

int
ha_glacier::pull_batch()
{
  int rc;

  release_batch();
  if (result_ == NULL)
    return HA_ERR_END_OF_FILE;
  rc = glacier_result_next_arrow(result_, &array_, &schema_);
  if (rc == 0)
    return HA_ERR_END_OF_FILE;
  if (rc != 1)
  {
    const char *msg = glacier_result_error(result_);
    std::string owned = (msg != NULL && msg[0] != '\0') ? msg : "reading the next batch failed";
    return fail(owned.c_str());
  }
  batch_len_ = array_.length;
  batch_pos_ = 0;
  return 0;
}

int
ha_glacier::rnd_next(uchar *)
{
  int rc;

  if (table->in_use && table->in_use->killed)
  {
    if (conn_)
      glacier_cancel(conn_);
    return HA_ERR_QUERY_INTERRUPTED;
  }
  while (batch_pos_ >= batch_len_)
  {
    rc = pull_batch();
    if (rc)
      return rc;
  }
  rc = write_record(batch_pos_);
  if (rc)
    return rc;
  if (current_row_ == UINT64_MAX)
    current_row_ = 0;
  else
    current_row_++;
  batch_pos_++;
  return 0;
}

int
ha_glacier::rnd_pos(uchar *buf, uchar *pos)
{
  uint64_t want;
  int rc;

  memcpy(&want, pos, sizeof(want));
  if (current_row_ == want)
  {
    if (batch_pos_ == 0)
      return HA_ERR_WRONG_COMMAND;
    return write_record(batch_pos_ - 1);
  }
  if (current_row_ != UINT64_MAX && want < current_row_)
  {
    rc = open_query();
    if (rc)
      return rc;
  }
  while (current_row_ != want)
  {
    rc = rnd_next(buf);
    if (rc)
      return rc;
  }
  return 0;
}

void
ha_glacier::position(const uchar *)
{
  memcpy(ref, &current_row_, sizeof(current_row_));
}

static struct st_mysql_storage_engine glacier_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

maria_declare_plugin(glacier){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &glacier_storage_engine,
    "Glacier",
    "Marcelo Tesla",
    "Read Parquet, Iceberg, and a Glacier warehouse",
    PLUGIN_LICENSE_GPL,
    glacier_plugin_init,
    NULL,
    0x000602,
    NULL,
    NULL,
    "0.6.2",
    MariaDB_PLUGIN_MATURITY_GAMMA} maria_declare_plugin_end;
