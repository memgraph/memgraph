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
#include "database/graph_db_datatypes.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/hashed_file_writer.hpp"
#include "storage/property_value.hpp"
#include "transactions/type.hpp"
#include "utils/scheduler.hpp"

namespace durability {

/** A database operation log for durability. Buffers and periodically serializes
 * small-granulation database operations (Ops).
 *
 * The order is not deterministic in a multithreaded scenario (multiple DB
 * transactions). This is fine, the recovery process should be immune to this
 * indeterminism.
 */
class WriteAheadLog {
 public:
  /** A single operation that needs to be written to the write-ahead log. Either
   * a transaction operation (start, commit or abort), or a database storage
   * operation being executed within a transaction. */
  class Op {
   public:
    /** Defines operation type. For each type the comment indicates which values
     * need to be stored. All ops have the transaction_id member, so that's
     * omitted in the comment. */
    enum class Type {
      TRANSACTION_BEGIN,
      TRANSACTION_COMMIT,
      TRANSACTION_ABORT,
      CREATE_VERTEX,        // vertex_id
      CREATE_EDGE,          // edge_id, from_vertex_id, to_vertex_id, edge_type
      SET_PROPERTY_VERTEX,  // vertex_id, property, property_value
      SET_PROPERTY_EDGE,    // edge_id, property, property_value
      // remove property is done by setting a PropertyValue::Null
      ADD_LABEL,      // vertex_id, label
      REMOVE_LABEL,   // vertex_id, label
      REMOVE_VERTEX,  // vertex_id
      REMOVE_EDGE,    // edge_id
      BUILD_INDEX     // label, property
    };

    Op() = default;
    Op(const Type &type, tx::transaction_id_t tx_id)
        : type_(type), transaction_id_(tx_id) {}

    // Members valid for every op.
    Type type_;
    tx::transaction_id_t transaction_id_;
    // Hash obtained from the HashedFileWriter obtained after writing all the Op
    // values. Cumulative with previous writes.
    uint32_t hash_;

    // Members valid only for some ops, see Op::Type comments above.
    int64_t vertex_id_;
    int64_t edge_id_;
    int64_t vertex_from_id_;
    int64_t vertex_to_id_;
    std::string edge_type_;
    std::string property_;
    PropertyValue value_ = PropertyValue::Null;
    std::string label_;

    void Encode(
        HashedFileWriter &writer,
        communication::bolt::PrimitiveEncoder<HashedFileWriter> &encoder) const;

   public:
    /** Attempts to decode a WAL::Op from the given decoder. Returns the decoded
     * value if successful, otherwise returns nullopt. */
    static std::experimental::optional<Op> Decode(
        HashedFileReader &reader,
        communication::bolt::Decoder<HashedFileReader> &decoder);
  };

  WriteAheadLog(const std::experimental::filesystem::path &durability_dir,
                bool durability_enabled);
  ~WriteAheadLog();

  /** Enables the WAL. Called at the end of GraphDb construction, after
   * (optional) recovery. */
  void Enable() { enabled_ = true; }

  void TxBegin(tx::transaction_id_t tx_id);
  void TxCommit(tx::transaction_id_t tx_id);
  void TxAbort(tx::transaction_id_t tx_id);
  void CreateVertex(tx::transaction_id_t tx_id, int64_t vertex_id);
  void CreateEdge(tx::transaction_id_t tx_id, int64_t edge_id,
                  int64_t vertex_from_id, int64_t vertex_to_id,
                  const std::string &edge_type);
  void PropsSetVertex(tx::transaction_id_t tx_id, int64_t vertex_id,
                      const std::string &property, const PropertyValue &value);
  void PropsSetEdge(tx::transaction_id_t tx_id, int64_t edge_id,
                    const std::string &property, const PropertyValue &value);
  void AddLabel(tx::transaction_id_t tx_id, int64_t vertex_id,
                const std::string &label);
  void RemoveLabel(tx::transaction_id_t tx_id, int64_t vertex_id,
                   const std::string &label);
  void RemoveVertex(tx::transaction_id_t tx_id, int64_t vertex_id);
  void RemoveEdge(tx::transaction_id_t tx_id, int64_t edge_id);
  void BuildIndex(tx::transaction_id_t tx_id, const std::string &label,
                  const std::string &property);

 private:
  /** Groups the logic of WAL file handling (flushing, naming, rotating) */
  class WalFile {
   public:
    WalFile(const std::experimental::filesystem::path &wal__dir);
    ~WalFile();

    /** Initializes the WAL file. Must be called before first flush. Can be
     * called after Flush() to re-initialize stuff.  */
    void Init();

    /** Flushes all the ops in the buffer to the WAL file. If necessary rotates
     * the file. */
    void Flush(RingBuffer<Op> &buffer);

   private:
    const std::experimental::filesystem::path wal_dir_;
    HashedFileWriter writer_;
    communication::bolt::PrimitiveEncoder<HashedFileWriter> encoder_{writer_};

    // The file to which the WAL flushes data. The path is fixed, the file gets
    // moved when the WAL gets rotated.
    std::experimental::filesystem::path current_wal_file_;

    // Number of Ops in the current wal file.
    int current_wal_file_ops_count_{0};

    // The latest transaction whose delta is recorded in the current WAL file.
    // Zero indicates that no deltas have so far been written to the current WAL
    // file.
    tx::transaction_id_t latest_tx_{0};

    void RotateFile();
  };

  RingBuffer<Op> ops_;
  Scheduler scheduler_;
  WalFile wal_file_;
  // Used for disabling the WAL during DB recovery.
  bool enabled_{false};

  // Emplaces the given Op onto the buffer, if the WAL is enabled.
  void Emplace(Op &&op);
};
}  // namespace durability
