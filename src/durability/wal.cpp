#include "wal.hpp"

#include "utils/flag_validation.hpp"

DEFINE_int32(wal_flush_interval_millis, -1,
             "Interval between two write-ahead log flushes, in milliseconds. "
             "Set to -1 to disable the WAL.");

DEFINE_string(wal_directory, "wal",
              "Directory in which the write-ahead log files are stored.");

DEFINE_int32(wal_rotate_ops_count, 10000,
             "How many write-ahead ops should be stored in a single WAL file "
             "before rotating it.");

DEFINE_VALIDATED_int32(wal_buffer_size, 4096, "Write-ahead log buffer size.",
                       FLAG_IN_RANGE(1, 1 << 30));
