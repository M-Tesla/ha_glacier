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

/* Storage engine for MariaDB. It links libglacier.so and does not include
 * Glacier source. MySQL does not load this shared library: the plugin ABI
 * is MariaDB's. */

#include "my_global.h"
#include "thr_lock.h"
#include "handler.h"
#include "my_base.h"
#include "glacier.h"

class Glacier_share : public Handler_share {
public:
  mysql_mutex_t mutex;
  THR_LOCK lock;
  Glacier_share();
  ~Glacier_share()
  {
    thr_lock_delete(&lock);
    mysql_mutex_destroy(&mutex);
  }
};

class ha_glacier : public handler {
  THR_LOCK_DATA lock_;
  Glacier_share *share_;
  char *path_;
  GlacierDatabase *db_;
  GlacierConn *conn_;
  GlacierResult *result_;
  struct ArrowArray array_;
  struct ArrowSchema schema_;
  int64_t batch_pos_;
  int64_t batch_len_;
  uint64_t current_row_;

  Glacier_share *get_share();
  void release_batch();
  void release_scan();
  int fail(const char *msg);
  int fail_free(char *err);
  int open_query();
  int pull_batch();
  int write_record(int64_t batch_row);

public:
  ha_glacier(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_glacier() override;

  ulonglong table_flags() const override
  {
    return HA_NO_TRANSACTIONS | HA_REC_NOT_IN_SEQ | HA_BINLOG_STMT_CAPABLE;
  }
  ulong index_flags(uint, uint, bool) const override { return 0; }
  uint max_supported_record_length() const override { return HA_MAX_REC_LENGTH; }
  uint max_supported_keys() const override { return 0; }
  uint max_supported_key_parts() const override { return 0; }
  uint max_supported_key_length() const override { return 0; }

  int open(const char *name, int mode, uint test_if_locked) override;
  int close(void) override;
  int write_row(const uchar *buf) override;
  int update_row(const uchar *old_data, const uchar *new_data) override;
  int delete_row(const uchar *buf) override;
  int rnd_init(bool scan) override;
  int rnd_end() override;
  int rnd_next(uchar *buf) override;
  int rnd_pos(uchar *buf, uchar *pos) override;
  void position(const uchar *record) override;
  int info(uint) override;
  int external_lock(THD *thd, int lock_type) override;
  int delete_all_rows(void) override;
  int create(const char *name, TABLE *form, HA_CREATE_INFO *info) override;
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override;
};
