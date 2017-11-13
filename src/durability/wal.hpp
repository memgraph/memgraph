#pragma once

#include <chrono>
#include <cstdint>
#include <experimental/filesystem>
#include <experimental/optional>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/primitive_encoder.hpp"
#include "data_structures/ring_buffer.hpp"
#include "database/graph_db_datatypes.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/hashed_file_writer.hpp"
#include "storage/property_value.hpp"
#include "transactions/type.hpp"
#include "utils/datetime/timestamp.hpp"
#include "utils/scheduler.hpp"
#include "utils/timer.hpp"

// The amount of time between two flushes of the write-ahead log,
// in milliseconds.
DECLARE_int32(wal_flush_interval_millis);

// Directory in which the WAL is dumped.
DECLARE_string(wal_directory);

// How many Ops are stored in a single WAL file.
DECLARE_int32(wal_rotate_ops_count);

// The WAL buffer size (number of ops in a buffer).
DECLARE_int32(wal_buffer_size);

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

    void Encode(HashedFileWriter &writer,
                communication::bolt::PrimitiveEncoder<HashedFileWriter>
                    &encoder) const {
      encoder.WriteInt(static_cast<int64_t>(type_));
      encoder.WriteInt(static_cast<int64_t>(transaction_id_));

      switch (type_) {
        case Type::TRANSACTION_BEGIN:
        case Type::TRANSACTION_COMMIT:
        case Type::TRANSACTION_ABORT:
          break;
        case Type::CREATE_VERTEX:
          encoder.WriteInt(vertex_id_);
          break;
        case Type::CREATE_EDGE:
          encoder.WriteInt(edge_id_);
          encoder.WriteInt(vertex_from_id_);
          encoder.WriteInt(vertex_to_id_);
          encoder.WriteString(edge_type_);
          break;
        case Type::SET_PROPERTY_VERTEX:
          encoder.WriteInt(vertex_id_);
          encoder.WriteString(property_);
          encoder.WritePropertyValue(value_);
          break;
        case Type::SET_PROPERTY_EDGE:
          encoder.WriteInt(edge_id_);
          encoder.WriteString(property_);
          encoder.WritePropertyValue(value_);
          break;
        case Type::ADD_LABEL:
        case Type::REMOVE_LABEL:
          encoder.WriteInt(vertex_id_);
          encoder.WriteString(label_);
          break;
        case Type::REMOVE_VERTEX:
          encoder.WriteInt(vertex_id_);
          break;
        case Type::REMOVE_EDGE:
          encoder.WriteInt(edge_id_);
          break;
        case Type::BUILD_INDEX:
          encoder.WriteString(label_);
          encoder.WriteString(property_);
          break;
      }

      writer.WriteValue(writer.hash());
    }

#define DECODE_MEMBER(member, value_f)         \
  if (!decoder.ReadValue(&dv)) return nullopt; \
  r_val.member = dv.value_f();

   public:
    /** Attempts to decode a WAL::Op from the given decoder. Returns the decoded
     * value if successful, otherwise returns nullopt. */
    static std::experimental::optional<Op> Decode(
        HashedFileReader &reader,
        communication::bolt::Decoder<HashedFileReader> &decoder) {
      using std::experimental::nullopt;

      Op r_val;
      // The decoded value used as a temporary while decoding.
      communication::bolt::DecodedValue dv;

      try {
        if (!decoder.ReadValue(&dv)) return nullopt;
        r_val.type_ = static_cast<Op::Type>(dv.ValueInt());
        DECODE_MEMBER(transaction_id_, ValueInt)

        switch (r_val.type_) {
          case Type::TRANSACTION_BEGIN:
          case Type::TRANSACTION_COMMIT:
          case Type::TRANSACTION_ABORT:
            break;
          case Type::CREATE_VERTEX:
            DECODE_MEMBER(vertex_id_, ValueInt)
            break;
          case Type::CREATE_EDGE:
            DECODE_MEMBER(edge_id_, ValueInt)
            DECODE_MEMBER(vertex_from_id_, ValueInt)
            DECODE_MEMBER(vertex_to_id_, ValueInt)
            DECODE_MEMBER(edge_type_, ValueString)
            break;
          case Type::SET_PROPERTY_VERTEX:
            DECODE_MEMBER(vertex_id_, ValueInt)
            DECODE_MEMBER(property_, ValueString)
            if (!decoder.ReadValue(&dv)) return nullopt;
            r_val.value_ = static_cast<PropertyValue>(dv);
            break;
          case Type::SET_PROPERTY_EDGE:
            DECODE_MEMBER(edge_id_, ValueInt)
            DECODE_MEMBER(property_, ValueString)
            if (!decoder.ReadValue(&dv)) return nullopt;
            r_val.value_ = static_cast<PropertyValue>(dv);
            break;
          case Type::ADD_LABEL:
          case Type::REMOVE_LABEL:
            DECODE_MEMBER(vertex_id_, ValueInt)
            DECODE_MEMBER(label_, ValueString)
            break;
          case Type::REMOVE_VERTEX:
            DECODE_MEMBER(vertex_id_, ValueInt)
            break;
          case Type::REMOVE_EDGE:
            DECODE_MEMBER(edge_id_, ValueInt)
            break;
          case Type::BUILD_INDEX:
            DECODE_MEMBER(label_, ValueString)
            DECODE_MEMBER(property_, ValueString)
            break;
        }

        auto decoder_hash = reader.hash();
        uint64_t encoded_hash;
        if (!reader.ReadType(encoded_hash, true)) return nullopt;
        if (decoder_hash != encoded_hash) return nullopt;

        return r_val;
      } catch (communication::bolt::DecodedValueException &) {
        return nullopt;
      } catch (std::ifstream::failure &) {
        return nullopt;
      }
    }
  };

#undef DECODE_MEMBER

  WriteAheadLog() {
    if (FLAGS_wal_flush_interval_millis >= 0) {
      wal_file_.Init();
      scheduler_.Run(std::chrono::milliseconds(FLAGS_wal_flush_interval_millis),
                     [this]() { wal_file_.Flush(ops_); });
    }
  }

  ~WriteAheadLog() {
    if (FLAGS_wal_flush_interval_millis >= 0) {
      scheduler_.Stop();
      wal_file_.Flush(ops_);
    }
  }

  /** Enables the WAL. Called at the end of GraphDb construction, after
   * (optional) recovery. */
  void Enable() { enabled_ = true; }

  void TxBegin(tx::transaction_id_t tx_id) {
    Emplace({Op::Type::TRANSACTION_BEGIN, tx_id});
  }

  void TxCommit(tx::transaction_id_t tx_id) {
    Emplace({Op::Type::TRANSACTION_COMMIT, tx_id});
  }

  void TxAbort(tx::transaction_id_t tx_id) {
    Emplace({Op::Type::TRANSACTION_ABORT, tx_id});
  }

  void CreateVertex(tx::transaction_id_t tx_id, int64_t vertex_id) {
    Op op(Op::Type::CREATE_VERTEX, tx_id);
    op.vertex_id_ = vertex_id;
    Emplace(std::move(op));
  }

  void CreateEdge(tx::transaction_id_t tx_id, int64_t edge_id,
                  int64_t vertex_from_id, int64_t vertex_to_id,
                  const std::string &edge_type) {
    Op op(Op::Type::CREATE_EDGE, tx_id);
    op.edge_id_ = edge_id;
    op.vertex_from_id_ = vertex_from_id;
    op.vertex_to_id_ = vertex_to_id;
    op.edge_type_ = edge_type;
    Emplace(std::move(op));
  }

  void PropsSetVertex(tx::transaction_id_t tx_id, int64_t vertex_id,
                      const std::string &property, const PropertyValue &value) {
    Op op(Op::Type::SET_PROPERTY_VERTEX, tx_id);
    op.vertex_id_ = vertex_id;
    op.property_ = property;
    op.value_ = value;
    Emplace(std::move(op));
  }

  void PropsSetEdge(tx::transaction_id_t tx_id, int64_t edge_id,
                    const std::string &property, const PropertyValue &value) {
    Op op(Op::Type::SET_PROPERTY_EDGE, tx_id);
    op.edge_id_ = edge_id;
    op.property_ = property;
    op.value_ = value;
    Emplace(std::move(op));
  }

  void AddLabel(tx::transaction_id_t tx_id, int64_t vertex_id,
                const std::string &label) {
    Op op(Op::Type::ADD_LABEL, tx_id);
    op.vertex_id_ = vertex_id;
    op.label_ = label;
    Emplace(std::move(op));
  }

  void RemoveLabel(tx::transaction_id_t tx_id, int64_t vertex_id,
                   const std::string &label) {
    Op op(Op::Type::REMOVE_LABEL, tx_id);
    op.vertex_id_ = vertex_id;
    op.label_ = label;
    Emplace(std::move(op));
  }

  void RemoveVertex(tx::transaction_id_t tx_id, int64_t vertex_id) {
    Op op(Op::Type::REMOVE_VERTEX, tx_id);
    op.vertex_id_ = vertex_id;
    Emplace(std::move(op));
  }

  void RemoveEdge(tx::transaction_id_t tx_id, int64_t edge_id) {
    Op op(Op::Type::REMOVE_EDGE, tx_id);
    op.edge_id_ = edge_id;
    Emplace(std::move(op));
  }

  void BuildIndex(tx::transaction_id_t tx_id, const std::string &label,
                  const std::string &property) {
    Op op(Op::Type::BUILD_INDEX, tx_id);
    op.label_ = label;
    op.property_ = property;
    Emplace(std::move(op));
  }

 private:
  /** Groups the logic of WAL file handling (flushing, naming, rotating) */
  class WalFile {
    using path = std::experimental::filesystem::path;

   public:
    ~WalFile() {
      if (!current_wal_file_.empty()) writer_.Close();
    }

    /** Initializes the WAL file. Must be called before first flush. Can be
     * called after Flush() to re-initialize stuff.  */
    void Init() {
      if (!std::experimental::filesystem::exists(FLAGS_wal_directory) &&
          !std::experimental::filesystem::create_directories(
              FLAGS_wal_directory)) {
        LOG(ERROR) << "Can't write to WAL directory: " << FLAGS_wal_directory;
        current_wal_file_ = path();
      } else {
        current_wal_file_ = MakeFilePath("__current");
        try {
          writer_.Open(current_wal_file_);
        } catch (std::ios_base::failure &) {
          LOG(ERROR) << "Failed to open write-ahead log file: "
                     << current_wal_file_;
          current_wal_file_ = path();
        }
      }
      latest_tx_ = 0;
      current_wal_file_ops_count_ = 0;
    }

    /** Flushes all the ops in the buffer to the WAL file. If necessary rotates
     * the file. */
    void Flush(RingBuffer<Op> &buffer) {
      if (current_wal_file_.empty()) {
        LOG(ERROR) << "Write-ahead log file uninitialized, discarding data.";
        buffer.clear();
        return;
      }

      try {
        while (true) {
          auto op = buffer.pop();
          if (!op) break;
          latest_tx_ = std::max(latest_tx_, op->transaction_id_);
          op->Encode(writer_, encoder_);
          if (++current_wal_file_ops_count_ >= FLAGS_wal_rotate_ops_count)
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

   private:
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

    path MakeFilePath(const std::string &suffix) {
      return path(FLAGS_wal_directory) /
             (Timestamp::now().to_iso8601() + suffix);
    }

    void RotateFile() {
      writer_.Close();
      std::experimental::filesystem::rename(
          current_wal_file_,
          MakeFilePath("__max_transaction_" + std::to_string(latest_tx_)));
      Init();
    }
  };

  RingBuffer<Op> ops_{FLAGS_wal_buffer_size};
  Scheduler scheduler_;
  WalFile wal_file_;
  // Used for disabling the WAL during DB recovery.
  bool enabled_{false};

  // Emplaces the given Op onto the buffer, if the WAL is enabled.
  void Emplace(Op &&op) {
    if (enabled_ && FLAGS_wal_flush_interval_millis >= 0)
      ops_.emplace(std::move(op));
  }
};
}  // namespace durability
