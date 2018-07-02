#include "wal.hpp"

#include "durability/paths.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"

DEFINE_HIDDEN_int32(
    wal_flush_interval_millis, 2,
    "Interval between two write-ahead log flushes, in milliseconds.");

DEFINE_HIDDEN_int32(
    wal_rotate_deltas_count, 10000,
    "How many write-ahead deltas should be stored in a single WAL file "
    "before rotating it.");

DEFINE_VALIDATED_HIDDEN_int32(wal_buffer_size, 4096,
                              "Write-ahead log buffer size.",
                              FLAG_IN_RANGE(1, 1 << 30));

namespace durability {

WriteAheadLog::WriteAheadLog(
    int worker_id, const std::experimental::filesystem::path &durability_dir,
    bool durability_enabled)
    : deltas_{FLAGS_wal_buffer_size}, wal_file_{worker_id, durability_dir} {
  if (durability_enabled) {
    utils::CheckDir(durability_dir);
    wal_file_.Init();
    scheduler_.Run("WAL",
                   std::chrono::milliseconds(FLAGS_wal_flush_interval_millis),
                   [this]() { wal_file_.Flush(deltas_); });
  }
}

WriteAheadLog::~WriteAheadLog() {
  // TODO review : scheduler.Stop() legal if it wasn't started?
  scheduler_.Stop();
  if (enabled_) wal_file_.Flush(deltas_);
}

WriteAheadLog::WalFile::WalFile(
    int worker_id, const std::experimental::filesystem::path &durability_dir)
    : worker_id_(worker_id), wal_dir_{durability_dir / kWalDir} {}

WriteAheadLog::WalFile::~WalFile() {
  if (!current_wal_file_.empty()) writer_.Close();
}

void WriteAheadLog::WalFile::Init() {
  if (!utils::EnsureDir(wal_dir_)) {
    LOG(ERROR) << "Can't write to WAL directory: " << wal_dir_;
    current_wal_file_ = std::experimental::filesystem::path();
  } else {
    current_wal_file_ = WalFilenameForTransactionId(wal_dir_, worker_id_);
    try {
      writer_.Open(current_wal_file_);
    } catch (std::ios_base::failure &) {
      LOG(ERROR) << "Failed to open write-ahead log file: "
                 << current_wal_file_;
      current_wal_file_ = std::experimental::filesystem::path();
    }
  }
  latest_tx_ = 0;
  current_wal_file_delta_count_ = 0;
}

void WriteAheadLog::WalFile::Flush(RingBuffer<database::StateDelta> &buffer) {
  std::lock_guard<std::mutex> flush_lock(flush_mutex_);
  if (current_wal_file_.empty()) {
    LOG(ERROR) << "Write-ahead log file uninitialized, discarding data.";
    buffer.clear();
    return;
  }

  try {
    while (true) {
      auto delta = buffer.pop();
      if (!delta) break;
      latest_tx_ = std::max(latest_tx_, delta->transaction_id);
      delta->Encode(writer_, encoder_);
      if (++current_wal_file_delta_count_ >= FLAGS_wal_rotate_deltas_count)
        RotateFile();
    }
    writer_.Flush();
  } catch (std::ios_base::failure &) {
    LOG(ERROR) << "Failed to write to write-ahead log, discarding data.";
    buffer.clear();
    return;
  } catch (std::experimental::filesystem::filesystem_error &) {
    LOG(ERROR) << "Failed to rotate write-ahead log.";
    buffer.clear();
    return;
  }
}

void WriteAheadLog::WalFile::RotateFile() {
  writer_.Close();
  std::experimental::filesystem::rename(
      current_wal_file_,
      WalFilenameForTransactionId(wal_dir_, worker_id_, latest_tx_));
  Init();
}

void WriteAheadLog::Emplace(database::StateDelta &&delta) {
  if (enabled_ && FLAGS_wal_flush_interval_millis >= 0)
    deltas_.emplace(std::move(delta));
}

void WriteAheadLog::Emplace(const database::StateDelta &delta) {
  if (enabled_ && FLAGS_wal_flush_interval_millis >= 0) deltas_.emplace(delta);
}

void WriteAheadLog::Flush() {
  if (enabled_) wal_file_.Flush(deltas_);
}
}  // namespace durability
