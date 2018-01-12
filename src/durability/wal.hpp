#pragma once

#include <chrono>
#include <cstdint>
#include <experimental/filesystem>
#include <experimental/optional>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/primitive_encoder.hpp"
#include "data_structures/ring_buffer.hpp"
#include "database/state_delta.hpp"
#include "database/types.hpp"
#include "storage/gid.hpp"
#include "storage/property_value.hpp"
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
  WriteAheadLog(const std::experimental::filesystem::path &durability_dir,
                bool durability_enabled);
  ~WriteAheadLog();

  /** Enables the WAL. Called at the end of database::GraphDb construction,
   * after
   * (optional) recovery. */
  void Enable() { enabled_ = true; }

  // Emplaces the given DeltaState onto the buffer, if the WAL is enabled.
  void Emplace(database::StateDelta &&delta);

 private:
  /** Groups the logic of WAL file handling (flushing, naming, rotating) */
  class WalFile {
   public:
    explicit WalFile(const std::experimental::filesystem::path &wal__dir);
    ~WalFile();

    /** Initializes the WAL file. Must be called before first flush. Can be
     * called after Flush() to re-initialize stuff.  */
    void Init();

    /** Flushes all the deltas in the buffer to the WAL file. If necessary
     * rotates the file. */
    void Flush(RingBuffer<database::StateDelta> &buffer);

   private:
    const std::experimental::filesystem::path wal_dir_;
    HashedFileWriter writer_;
    communication::bolt::PrimitiveEncoder<HashedFileWriter> encoder_{writer_};

    // The file to which the WAL flushes data. The path is fixed, the file gets
    // moved when the WAL gets rotated.
    std::experimental::filesystem::path current_wal_file_;

    // Number of deltas in the current wal file.
    int current_wal_file_delta_count_{0};

    // The latest transaction whose delta is recorded in the current WAL file.
    // Zero indicates that no deltas have so far been written to the current WAL
    // file.
    tx::transaction_id_t latest_tx_{0};

    void RotateFile();
  };

  RingBuffer<database::StateDelta> deltas_;
  Scheduler scheduler_;
  WalFile wal_file_;
  // Used for disabling the WAL during DB recovery.
  bool enabled_{false};
};
}  // namespace durability
