#include "wal.hpp"

#include "durability/single_node/paths.hpp"
#include "durability/single_node/version.hpp"
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
    const std::experimental::filesystem::path &durability_dir,
    bool durability_enabled, bool synchronous_commit)
    : deltas_{FLAGS_wal_buffer_size},
      wal_file_{durability_dir},
      durability_enabled_(durability_enabled),
      synchronous_commit_(synchronous_commit) {
  if (durability_enabled_) {
    utils::CheckDir(durability_dir);
  }
}

WriteAheadLog::~WriteAheadLog() {
  if (durability_enabled_) {
    if (!synchronous_commit_) scheduler_.Stop();
    wal_file_.Flush(deltas_);
  }
}

WriteAheadLog::WalFile::WalFile(
    const std::experimental::filesystem::path &durability_dir)
    : wal_dir_{durability_dir / kWalDir} {}

WriteAheadLog::WalFile::~WalFile() {
  if (!current_wal_file_.empty()) writer_.Close();
}

void WriteAheadLog::WalFile::Init() {
  if (!utils::EnsureDir(wal_dir_)) {
    LOG(ERROR) << "Can't write to WAL directory: " << wal_dir_;
    current_wal_file_ = std::experimental::filesystem::path();
  } else {
    current_wal_file_ = WalFilenameForTransactionId(wal_dir_);
    // TODO: Fix error handling, the encoder_ returns `true` or `false`.
    try {
      writer_.Open(current_wal_file_);
      encoder_.WriteRAW(durability::kWalMagic.data(),
                        durability::kWalMagic.size());
      encoder_.WriteInt(durability::kVersion);
      writer_.Flush();
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
      writer_.Flush();
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
  writer_.Flush();
  writer_.Close();
  std::experimental::filesystem::rename(
      current_wal_file_,
      WalFilenameForTransactionId(wal_dir_, latest_tx_));
  Init();
}

void WriteAheadLog::Init() {
  if (durability_enabled_) {
    enabled_ = true;
    wal_file_.Init();
    if (!synchronous_commit_) {
      scheduler_.Run("WAL",
                     std::chrono::milliseconds(FLAGS_wal_flush_interval_millis),
                     [this]() { wal_file_.Flush(deltas_); });
    }
  }
}

void WriteAheadLog::Emplace(const database::StateDelta &delta) {
  if (durability_enabled_ && enabled_) {
    deltas_.emplace(delta);
    if (synchronous_commit_ && IsStateDeltaTransactionEnd(delta)) {
      wal_file_.Flush(deltas_);
    }
  }
}

bool WriteAheadLog::IsStateDeltaTransactionEnd(
    const database::StateDelta &delta) {
  switch (delta.type) {
    case database::StateDelta::Type::TRANSACTION_COMMIT:
    case database::StateDelta::Type::TRANSACTION_ABORT:
      return true;
    case database::StateDelta::Type::TRANSACTION_BEGIN:
    case database::StateDelta::Type::CREATE_VERTEX:
    case database::StateDelta::Type::CREATE_EDGE:
    case database::StateDelta::Type::SET_PROPERTY_VERTEX:
    case database::StateDelta::Type::SET_PROPERTY_EDGE:
    case database::StateDelta::Type::ADD_LABEL:
    case database::StateDelta::Type::REMOVE_LABEL:
    case database::StateDelta::Type::REMOVE_VERTEX:
    case database::StateDelta::Type::REMOVE_EDGE:
    case database::StateDelta::Type::BUILD_INDEX:
      return false;
  }
}

void WriteAheadLog::Flush() {
  if (enabled_) {
    wal_file_.Flush(deltas_);
  }
}
}  // namespace durability
