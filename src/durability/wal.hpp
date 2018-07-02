#pragma once

#include <chrono>
#include <cstdint>
#include <experimental/filesystem>
#include <experimental/optional>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/encoder/base_encoder.hpp"
#include "data_structures/ring_buffer.hpp"
#include "database/state_delta.hpp"
#include "storage/gid.hpp"
#include "storage/property_value.hpp"
#include "storage/types.hpp"
#include "transactions/type.hpp"
#include "utils/scheduler.hpp"

namespace durability {

/** A database StateDelta log for durability. Buffers and periodically
 * serializes small-granulation database deltas (StateDelta).
 *
 * The order is not deterministic in a multithreaded scenario (multiple DB
 * transactions). This is fine, the recovery process should be immune to this
 * indeterminism.
 */
class WriteAheadLog {
 public:
  WriteAheadLog(int worker_id,
                const std::experimental::filesystem::path &durability_dir,
                bool durability_enabled);
  ~WriteAheadLog();

  /** Enables the WAL. Called at the end of GraphDb construction, after
   * (optional) recovery. */
  void Enable() { enabled_ = true; }

  /// Emplaces the given DeltaState onto the buffer, if the WAL is enabled.
  void Emplace(database::StateDelta &&delta);

  /// Emplaces the given DeltaState onto the buffer, if the WAL is enabled.
  void Emplace(const database::StateDelta &delta);

  /// Flushes every delta currently in the ring buffer
  void Flush();

 private:
  /** Groups the logic of WAL file handling (flushing, naming, rotating) */
  class WalFile {
   public:
    WalFile(int worker_id, const std::experimental::filesystem::path &wal__dir);
    ~WalFile();

    /** Initializes the WAL file. Must be called before first flush. Can be
     * called after Flush() to re-initialize stuff.  */
    void Init();

    /** Flushes all the deltas in the buffer to the WAL file. If necessary
     * rotates the file. */
    void Flush(RingBuffer<database::StateDelta> &buffer);

   private:
    // Mutex used for flushing wal data
    std::mutex flush_mutex_;
    int worker_id_;
    const std::experimental::filesystem::path wal_dir_;
    HashedFileWriter writer_;
    communication::bolt::BaseEncoder<HashedFileWriter> encoder_{writer_};

    // The file to which the WAL flushes data. The path is fixed, the file gets
    // moved when the WAL gets rotated.
    std::experimental::filesystem::path current_wal_file_;

    // Number of deltas in the current wal file.
    int current_wal_file_delta_count_{0};

    // The latest transaction whose delta is recorded in the current WAL file.
    // Zero indicates that no deltas have so far been written to the current WAL
    // file.
    tx::TransactionId latest_tx_{0};

    void RotateFile();
  };

  RingBuffer<database::StateDelta> deltas_;
  utils::Scheduler scheduler_;
  WalFile wal_file_;
  // Used for disabling the WAL during DB recovery.
  bool enabled_{false};
};
}  // namespace durability
