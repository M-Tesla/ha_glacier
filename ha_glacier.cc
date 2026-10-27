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
#include "sql_select.h"
#include "select_handler.h"
#include "item_func.h"
#include "item_sum.h"
#include "unireg.h"

#include <sys/stat.h>
#include <vector>

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
static handlerton *glacier_hton = NULL;
static int glacier_row_fail(const char *msg);
static select_handler *glacier_create_select(THD *thd, SELECT_LEX *select_lex,
                                             SELECT_LEX_UNIT *select_lex_unit);

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

struct ha_table_option_struct {
  char *path;
};

static ha_create_table_option glacier_table_options[] = {
    HA_TOPTION_STRING("CONNECTION", path), HA_TOPTION_END};

static const char glacier_need_path[] =
    "table needs CONNECTION='...' of a Parquet file, an Iceberg table, or a warehouse";

static int
glacier_plugin_init(void *p)
{
  handlerton *hton = (handlerton *) p;

  init_glacier_psi();
  glacier_hton = hton;
  hton->create = glacier_create_handler;
  hton->create_select = glacier_create_select;
  hton->table_options = glacier_table_options;
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

static void trim_inplace(char *s);

static bool
path_blank(const char *src)
{
  if (src == NULL)
    return true;
  while (*src == ' ' || *src == '\t' || *src == '\n' || *src == '\r')
    src++;
  return *src == '\0';
}

static int
copy_path(char **dest, const char *src)
{
  size_t n;
  char *copy;

  if (path_blank(src))
    return glacier_row_fail(glacier_need_path);
  n = strlen(src);
  if (n > 16384)
    return glacier_row_fail(glacier_need_path);
  copy = (char *) malloc(n + 1);
  if (copy == NULL)
    return HA_ERR_OUT_OF_MEM;
  memcpy(copy, src, n + 1);
  trim_inplace(copy);
  if (copy[0] == '\0')
  {
    free(copy);
    return glacier_row_fail(glacier_need_path);
  }
  free(*dest);
  *dest = copy;
  return 0;
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

static bool
path_needs_from(const char *path)
{
  size_t n;
  struct stat st;
  std::string meta;

  if (path == NULL)
    return false;
  n = strlen(path);
  if (n >= 8 && strcasecmp(path + n - 8, ".parquet") == 0)
    return false;
  if (n >= 5 && strcasecmp(path + n - 5, ".avro") == 0)
    return false;
  if (n >= 8 && strcasecmp(path + n - 8, ".glacier") == 0)
    return false;
  if (strstr(path, "://") != NULL)
    return true;
  if (stat(path, &st) != 0 || !S_ISDIR(st.st_mode))
    return false;
  meta = std::string(path) + "/metadata";
  if (stat(meta.c_str(), &st) == 0 && S_ISDIR(st.st_mode))
    return false;
  return true;
}

static Item *
bare_item(Item *item)
{
  if (item == NULL)
    return NULL;
  return item->real_item();
}

static bool
append_field(std::string *out, Item *item)
{
  Field *field;

  item = bare_item(item);
  if (item == NULL || item->type() != Item::FIELD_ITEM)
    return false;
  field = static_cast<Item_field *>(item)->field;
  if (field == NULL || !sql_ident(field->field_name.str, field->field_name.length))
    return false;
  out->append(field->field_name.str, field->field_name.length);
  return true;
}

static bool
append_literal(std::string *out, Item *item)
{
  String buf;
  String *s;
  size_t i;

  item = bare_item(item);
  if (item == NULL)
    return false;
  if (item->type() == Item::NULL_ITEM)
  {
    *out += "NULL";
    return true;
  }
  if (!item->const_item() || item->type() == Item::FIELD_ITEM)
    return false;
  s = item->val_str(&buf);
  if (item->null_value || s == NULL)
  {
    *out += "NULL";
    return true;
  }
  switch (item->result_type())
  {
    case INT_RESULT:
    case REAL_RESULT:
    case DECIMAL_RESULT:
      out->append(s->ptr(), s->length());
      return true;
    case STRING_RESULT:
      *out += "'";
      for (i = 0; i < s->length(); i++)
      {
        char c = s->ptr()[i];
        if (c == '\'')
          *out += "''";
        else if (c == '\0')
          return false;
        else
          *out += c;
      }
      *out += "'";
      return true;
    default:
      return false;
  }
}

static bool
deparse_sum(std::string *out, Item *item)
{
  Item_sum *sum;
  const char *name;

  item = bare_item(item);
  if (item == NULL || item->type() != Item::SUM_FUNC_ITEM)
    return false;
  sum = static_cast<Item_sum *>(item);
  if (sum->has_with_distinct())
    return false;
  switch (sum->sum_func())
  {
    case Item_sum::COUNT_FUNC:
      name = "COUNT";
      break;
    case Item_sum::SUM_FUNC:
      name = "SUM";
      break;
    case Item_sum::MIN_FUNC:
      name = "MIN";
      break;
    case Item_sum::MAX_FUNC:
      name = "MAX";
      break;
    case Item_sum::AVG_FUNC:
      name = "AVG";
      break;
    default:
      return false;
  }
  *out += name;
  *out += "(";
  if (sum->sum_func() == Item_sum::COUNT_FUNC &&
      (sum->argument_count() == 0 ||
       (sum->argument_count() == 1 && sum->arguments()[0]->const_item())))
    *out += "*";
  else if (sum->argument_count() == 1 && append_field(out, sum->arguments()[0]))
    ;
  else
    return false;
  *out += ")";
  return true;
}

static bool
deparse_expr(std::string *out, Item *item)
{
  item = bare_item(item);
  if (item == NULL)
    return false;
  if (item->type() == Item::FIELD_ITEM)
    return append_field(out, item);
  if (item->type() == Item::SUM_FUNC_ITEM)
    return deparse_sum(out, item);
  if (item->const_item())
    return append_literal(out, item);
  return false;
}

static bool
deparse_cond(std::string *out, Item *item)
{
  Item_func *func;
  const char *op;
  uint i;

  item = bare_item(item);
  if (item == NULL)
    return false;
  if (item->type() == Item::COND_ITEM || item->type() == Item::FUNC_ITEM)
  {
    func = static_cast<Item_func *>(item);
    if (func->functype() == Item_func::COND_AND_FUNC ||
        func->functype() == Item_func::COND_OR_FUNC)
    {
      op = func->functype() == Item_func::COND_AND_FUNC ? " AND " : " OR ";
      if (func->argument_count() == 0)
        return false;
      for (i = 0; i < func->argument_count(); i++)
      {
        if (i)
          *out += op;
        *out += "(";
        if (!deparse_cond(out, func->arguments()[i]))
          return false;
        *out += ")";
      }
      return true;
    }
    if (func->functype() == Item_func::ISNULL_FUNC && func->argument_count() == 1)
    {
      if (!append_field(out, func->arguments()[0]))
        return false;
      *out += " IS NULL";
      return true;
    }
    if (func->functype() == Item_func::ISNOTNULL_FUNC && func->argument_count() == 1)
    {
      if (!append_field(out, func->arguments()[0]))
        return false;
      *out += " IS NOT NULL";
      return true;
    }
    switch (func->functype())
    {
      case Item_func::EQ_FUNC:
      case Item_func::EQUAL_FUNC:
        op = " = ";
        break;
      case Item_func::NE_FUNC:
        op = " <> ";
        break;
      case Item_func::LT_FUNC:
        op = " < ";
        break;
      case Item_func::LE_FUNC:
        op = " <= ";
        break;
      case Item_func::GT_FUNC:
        op = " > ";
        break;
      case Item_func::GE_FUNC:
        op = " >= ";
        break;
      default:
        return false;
    }
    if (func->argument_count() != 2)
      return false;
    {
      std::string left;
      std::string right;
      bool ok = append_field(&left, func->arguments()[0]) &&
                append_literal(&right, func->arguments()[1]);
      if (!ok)
      {
        left.clear();
        right.clear();
        ok = append_literal(&left, func->arguments()[0]) &&
             append_field(&right, func->arguments()[1]);
      }
      if (!ok)
      {
        left.clear();
        right.clear();
        ok = append_field(&left, func->arguments()[0]) &&
             append_field(&right, func->arguments()[1]);
      }
      if (!ok)
        return false;
      *out += left;
      *out += op;
      *out += right;
      return true;
    }
  }
  return false;
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
  return glacier_row_fail(msg.c_str());
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
  const char *opt = (option_struct != NULL) ? option_struct->path : NULL;
  int rc;

  (void) name;
  (void) mode;
  (void) test_if_locked;
  if (!(share_ = get_share()))
    return HA_ERR_OUT_OF_MEM;
  thr_lock_data_init(&share_->lock, &lock_, NULL);
  rc = copy_path(&path_, opt);
  return rc;
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
ha_glacier::reset(void)
{
  where_.clear();
  return 0;
}

const COND *
ha_glacier::cond_push(const COND *cond)
{
  std::string sql;

  if (cond == NULL || !deparse_cond(&sql, const_cast<Item *>(cond)))
  {
    where_.clear();
    return cond;
  }
  where_ = sql;
  return NULL;
}

void
ha_glacier::cond_pop()
{
  where_.clear();
}

int
ha_glacier::create(const char *name, TABLE *, HA_CREATE_INFO *create_info)
{
  const char *opt;

  (void) name;
  opt = (create_info != NULL && create_info->option_struct != NULL)
            ? create_info->option_struct->path
            : NULL;
  if (path_blank(opt) || strlen(opt) > 16384)
    return glacier_row_fail(glacier_need_path);
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
    return glacier_row_fail("table has no CONNECTION path");
  if (glacier_api_version() != GLACIER_API_VERSION)
    return glacier_row_fail("libglacier API does not match this plugin");
  if (table->field == NULL)
    return glacier_row_fail("table has no columns");
  for (Field **fp = table->field; *fp; fp++)
  {
    Field *field = *fp;
    if (!sql_ident(field->field_name.str, field->field_name.length))
      return glacier_row_fail("column names must be letters, digits, and underscores");
    if (!first)
      sql += ", ";
    sql.append(field->field_name.str, field->field_name.length);
    first = false;
  }
  if (first)
    return glacier_row_fail("table has no columns");
  if (path_needs_from(path_))
  {
    if (!sql_ident(table->s->db.str, table->s->db.length) ||
        !sql_ident(table->s->table_name.str, table->s->table_name.length))
      return glacier_row_fail("database and table names must be letters, digits, and underscores");
    sql += " FROM ";
    sql.append(table->s->db.str, table->s->db.length);
    sql += ".";
    sql.append(table->s->table_name.str, table->s->table_name.length);
  }
  if (pushed_cond != NULL && where_.size() > 0)
  {
    sql += " WHERE ";
    sql += where_;
  }

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
    return glacier_row_fail(msg.c_str());
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

static int
glacier_row_fail(const char *msg)
{
  my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0),
                  (msg != NULL && msg[0] != '\0') ? msg : "failed");
  return HA_ERR_GENERIC;
}

static int
glacier_write_record(TABLE *dst, struct ArrowArray *rows, struct ArrowSchema *fields,
                      int64_t batch_row)
{
  MY_BITMAP *old_map;
  uint i;

  if (fields->children == NULL || rows->children == NULL)
  {
    my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0), "Arrow batch is missing its columns");
    return HA_ERR_GENERIC;
  }
  if (fields->n_children != (int64_t) dst->s->fields)
  {
    my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0),
                    "Glacier returned a different number of columns than the table");
    return HA_ERR_GENERIC;
  }

  empty_record(dst);
  old_map = tmp_use_all_columns(dst, &dst->write_set);
  for (i = 0; i < dst->s->fields; i++)
  {
    Field *field = dst->field[i];
    const struct ArrowSchema *fs = fields->children[i];
    const struct ArrowArray *col = rows->children[i];
    const char *fmt;
    int64_t ix;
    int rc = 0;

    if (fs == NULL || col == NULL || fs->format == NULL)
    {
      tmp_restore_column_map(&dst->write_set, old_map);
      return glacier_row_fail("Arrow batch is missing a column");
    }
    if (fs->dictionary != NULL || col->dictionary != NULL || fs->n_children != 0)
    {
      tmp_restore_column_map(&dst->write_set, old_map);
      return glacier_row_fail("nested or dictionary-encoded values are not supported");
    }
    if (arrow_is_null(rows, batch_row) || arrow_is_null(col, batch_row))
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
        tmp_restore_column_map(&dst->write_set, old_map);
        return glacier_row_fail("boolean column is missing its value buffer");
      }
      v = (bits[ix >> 3] >> (ix & 7)) & 1;
      rc = field->store((longlong) v, false);
    }
    else if (strcmp(fmt, "i") == 0)
    {
      const int32_t *v = (const int32_t *) arrow_buf(col, 1);
      if (v == NULL)
      {
        tmp_restore_column_map(&dst->write_set, old_map);
        return glacier_row_fail("int32 column is missing its value buffer");
      }
      rc = field->store((longlong) v[ix], (field->flags & UNSIGNED_FLAG) != 0);
    }
    else if (strcmp(fmt, "l") == 0)
    {
      const int64_t *v = (const int64_t *) arrow_buf(col, 1);
      if (v == NULL)
      {
        tmp_restore_column_map(&dst->write_set, old_map);
        return glacier_row_fail("int64 column is missing its value buffer");
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
          tmp_restore_column_map(&dst->write_set, old_map);
          return glacier_row_fail("float column is missing its value buffer");
        }
        d = (double) v[ix];
      }
      else
      {
        const double *v = (const double *) arrow_buf(col, 1);
        if (v == NULL)
        {
          tmp_restore_column_map(&dst->write_set, old_map);
          return glacier_row_fail("double column is missing its value buffer");
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
          tmp_restore_column_map(&dst->write_set, old_map);
          return glacier_row_fail("string column is missing its offset buffer");
        }
        start = off[ix];
        end = off[ix + 1];
      }
      else
      {
        const int64_t *off = (const int64_t *) arrow_buf(col, 1);
        if (off == NULL)
        {
          tmp_restore_column_map(&dst->write_set, old_map);
          return glacier_row_fail("string column is missing its offset buffer");
        }
        start = off[ix];
        end = off[ix + 1];
      }
      if (end < start)
      {
        tmp_restore_column_map(&dst->write_set, old_map);
        return glacier_row_fail("string column has a bad offset");
      }
      if (end == start)
        rc = store_text(field, "", 0);
      else if (bytes == NULL)
      {
        tmp_restore_column_map(&dst->write_set, old_map);
        return glacier_row_fail("string column is missing its byte buffer");
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
        tmp_restore_column_map(&dst->write_set, old_map);
        return glacier_row_fail("timestamp column is not supported");
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
          tmp_restore_column_map(&dst->write_set, old_map);
          return glacier_row_fail("timestamp is out of range");
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
        tmp_restore_column_map(&dst->write_set, old_map);
        return glacier_row_fail("decimal column is missing its value buffer");
      }
      sscanf(fmt + 2, "%*d,%d", &scale);
      n = i128_text(raw + (size_t) ix * 16, scale, textv, sizeof(textv));
      if (n < 0)
      {
        tmp_restore_column_map(&dst->write_set, old_map);
        return glacier_row_fail("decimal value does not fit");
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
        tmp_restore_column_map(&dst->write_set, old_map);
        return glacier_row_fail("fixed-width column is missing its value buffer");
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
      tmp_restore_column_map(&dst->write_set, old_map);
      snprintf(textv, sizeof(textv), "Arrow format \"%s\" is not supported", fmt);
      return glacier_row_fail(textv);
    }
    if (rc < 0)
    {
      tmp_restore_column_map(&dst->write_set, old_map);
      return glacier_row_fail("writing a column into the MariaDB row failed");
    }
  }
  tmp_restore_column_map(&dst->write_set, old_map);
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
    return glacier_row_fail(owned.c_str());
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
  rc = glacier_write_record(table, &array_, &schema_, batch_pos_);
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
    return glacier_write_record(table, &array_, &schema_, batch_pos_ - 1);
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

static bool
deparse_order_list(std::string *out, SQL_I_List<ORDER> *list, const char *kw)
{
  ORDER *ord;
  bool first = true;

  if (list == NULL || list->elements == 0)
    return true;
  *out += kw;
  for (ord = list->first; ord != NULL; ord = ord->next)
  {
    if (!first)
      *out += ", ";
    if (ord->item == NULL || !deparse_expr(out, *ord->item))
      return false;
    if (ord->direction == ORDER::ORDER_DESC)
      *out += " DESC";
    first = false;
  }
  return true;
}

static bool
glacier_deparse_select(SELECT_LEX *sel, std::string *sql, std::string *path)
{
  List_iterator_fast<Item> it(sel->item_list);
  Item *item;
  TABLE_LIST *tbl;
  ha_glacier *ha;
  bool first = true;
  int tables = 0;
  std::string from;
  std::string where;
  std::string having;
  longlong limit_n;
  longlong limit_off;

  if (sel == NULL || sel->olap != UNSPECIFIED_OLAP_TYPE || sel->limit_params.with_ties)
    return false;
  *sql = (sel->options & SELECT_DISTINCT) ? "SELECT DISTINCT " : "SELECT ";
  while ((item = it++))
  {
    if (item->type() == Item::WINDOW_FUNC_ITEM || item->type() == Item::SUBSELECT_ITEM)
      return false;
    if (!first)
      *sql += ", ";
    if (!deparse_expr(sql, item))
      return false;
    first = false;
  }
  if (first)
    return false;
  for (tbl = sel->join ? sel->join->tables_list : NULL; tbl != NULL; tbl = tbl->next_global)
  {
    if (tbl->table == NULL)
      continue;
    if (tbl->table->file == NULL || tbl->table->file->ht != glacier_hton)
      return false;
    ha = static_cast<ha_glacier *>(tbl->table->file);
    if (ha->open_path() == NULL || ha->open_path()[0] == '\0')
      return false;
    if (tables == 0)
      *path = ha->open_path();
    else if (*path != ha->open_path())
      return false;
    tables++;
    if (!path_needs_from(ha->open_path()))
      continue;
    if (!sql_ident(tbl->table->s->db.str, tbl->table->s->db.length) ||
        !sql_ident(tbl->table->s->table_name.str, tbl->table->s->table_name.length))
      return false;
    if (!from.empty())
      from += ", ";
    from.append(tbl->table->s->db.str, tbl->table->s->db.length);
    from += ".";
    from.append(tbl->table->s->table_name.str, tbl->table->s->table_name.length);
  }
  if (tables != 1)
    return false;
  if (!from.empty())
  {
    *sql += " FROM ";
    *sql += from;
  }
  if (sel->where != NULL)
  {
    if (!deparse_cond(&where, sel->where))
      return false;
    *sql += " WHERE ";
    *sql += where;
  }
  if (!deparse_order_list(sql, &sel->group_list, " GROUP BY "))
    return false;
  if (sel->having != NULL)
  {
    if (!deparse_cond(&having, sel->having))
      return false;
    *sql += " HAVING ";
    *sql += having;
  }
  if (!deparse_order_list(sql, &sel->order_list, " ORDER BY "))
    return false;
  if (sel->limit_params.explicit_limit)
  {
    if (sel->limit_params.select_limit == NULL ||
        !sel->limit_params.select_limit->const_item())
      return false;
    limit_n = sel->limit_params.select_limit->val_int();
    if (sel->limit_params.select_limit->null_value || limit_n < 0)
      return false;
    if (sel->limit_params.offset_limit != NULL)
    {
      if (!sel->limit_params.offset_limit->const_item())
        return false;
      limit_off = sel->limit_params.offset_limit->val_int();
      if (sel->limit_params.offset_limit->null_value || limit_off < 0)
        return false;
    }
    else
      limit_off = 0;
    *sql += " LIMIT ";
    *sql += std::to_string(limit_n);
    if (limit_off > 0)
    {
      *sql += " OFFSET ";
      *sql += std::to_string(limit_off);
    }
  }
  return true;
}

class ha_glacier_select : public select_handler {
  std::string sql_;
  std::string path_;
  GlacierDatabase *db_;
  GlacierConn *conn_;
  GlacierResult *result_;
  struct ArrowArray array_;
  struct ArrowSchema schema_;
  int64_t batch_pos_;
  int64_t batch_len_;

  void release_batch();
  void release_scan();

public:
  ha_glacier_select(THD *thd_arg, handlerton *ht_arg, SELECT_LEX *sel, std::string sql,
                    std::string path);
  ~ha_glacier_select() override;
  int init_scan() override;
  int next_row() override;
  int end_scan() override;
  void print_error(int error, myf errflag) override;
};

ha_glacier_select::ha_glacier_select(THD *thd_arg, handlerton *ht_arg, SELECT_LEX *sel,
                                     std::string sql, std::string path)
    : select_handler(thd_arg, ht_arg, sel), sql_(sql), path_(path), db_(NULL), conn_(NULL),
      result_(NULL), batch_pos_(0), batch_len_(0)
{
  memset(&array_, 0, sizeof(array_));
  memset(&schema_, 0, sizeof(schema_));
}

ha_glacier_select::~ha_glacier_select()
{
  release_scan();
}

void
ha_glacier_select::release_batch()
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
ha_glacier_select::release_scan()
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
}

void
ha_glacier_select::print_error(int error, myf errflag)
{
  if (!thd->is_error())
    select_handler::print_error(error, errflag);
}

int
ha_glacier_select::init_scan()
{
  char *err = NULL;

  release_scan();
  if (glacier_api_version() != GLACIER_API_VERSION)
  {
    my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0),
                    "libglacier API does not match this plugin");
    return HA_ERR_GENERIC;
  }
  db_ = glacier_open(path_.c_str(), &err);
  if (db_ == NULL)
  {
    std::string msg = (err != NULL && err[0] != '\0') ? err : "failed";
    glacier_free(err);
    my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0), msg.c_str());
    return HA_ERR_GENERIC;
  }
  conn_ = glacier_connect(db_, &err);
  if (conn_ == NULL)
  {
    std::string msg = (err != NULL && err[0] != '\0') ? err : "failed";
    glacier_free(err);
    release_scan();
    my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0), msg.c_str());
    return HA_ERR_GENERIC;
  }
  result_ = glacier_query(conn_, sql_.c_str(), &err);
  if (result_ == NULL)
  {
    std::string msg = (err != NULL && err[0] != '\0') ? err : "failed";
    glacier_free(err);
    release_scan();
    my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0), msg.c_str());
    return HA_ERR_GENERIC;
  }
  if (glacier_result_error(result_) != NULL)
  {
    std::string msg = glacier_result_error(result_);
    release_scan();
    my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0), msg.c_str());
    return HA_ERR_GENERIC;
  }
  return 0;
}

int
ha_glacier_select::next_row()
{
  int rc;

  if (thd->killed)
  {
    if (conn_)
      glacier_cancel(conn_);
    return HA_ERR_QUERY_INTERRUPTED;
  }
  while (batch_pos_ >= batch_len_)
  {
    release_batch();
    if (result_ == NULL)
      return HA_ERR_END_OF_FILE;
    rc = glacier_result_next_arrow(result_, &array_, &schema_);
    if (rc == 0)
      return HA_ERR_END_OF_FILE;
    if (rc != 1)
    {
      const char *msg = glacier_result_error(result_);
      my_printf_error(ER_UNKNOWN_ERROR, "Glacier: %s", MYF(0),
                      (msg != NULL && msg[0] != '\0') ? msg : "reading the next batch failed");
      return HA_ERR_GENERIC;
    }
    batch_len_ = array_.length;
    batch_pos_ = 0;
  }
  rc = glacier_write_record(table, &array_, &schema_, batch_pos_);
  if (rc)
    return rc;
  batch_pos_++;
  return 0;
}

int
ha_glacier_select::end_scan()
{
  release_scan();
  return 0;
}

static select_handler *
glacier_create_select(THD *thd, SELECT_LEX *select_lex, SELECT_LEX_UNIT *select_lex_unit)
{
  std::string sql;
  std::string path;

  (void) select_lex_unit;
  if (select_lex_unit != NULL)
    return NULL;
  if (!glacier_deparse_select(select_lex, &sql, &path))
    return NULL;
  return new (std::nothrow) ha_glacier_select(thd, glacier_hton, select_lex, sql, path);
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
    0x000700,
    NULL,
    NULL,
    "0.7.0",
    MariaDB_PLUGIN_MATURITY_GAMMA} maria_declare_plugin_end;
