#include "wal.hpp"

#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "utils/datetime/timestamp.hpp"
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

namespace durability {

void WriteAheadLog::Op::Encode(
    HashedFileWriter &writer,
    communication::bolt::PrimitiveEncoder<HashedFileWriter> &encoder) const {
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

std::experimental::optional<WriteAheadLog::Op> WriteAheadLog::Op::Decode(
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

#undef DECODE_MEMBER

WriteAheadLog::WriteAheadLog() {
  if (FLAGS_wal_flush_interval_millis >= 0) {
    wal_file_.Init();
    scheduler_.Run(std::chrono::milliseconds(FLAGS_wal_flush_interval_millis),
                   [this]() { wal_file_.Flush(ops_); });
  }
}

WriteAheadLog::~WriteAheadLog() {
  if (FLAGS_wal_flush_interval_millis >= 0) {
    scheduler_.Stop();
    wal_file_.Flush(ops_);
  }
}

void WriteAheadLog::TxBegin(tx::transaction_id_t tx_id) {
  Emplace({Op::Type::TRANSACTION_BEGIN, tx_id});
}

void WriteAheadLog::TxCommit(tx::transaction_id_t tx_id) {
  Emplace({Op::Type::TRANSACTION_COMMIT, tx_id});
}

void WriteAheadLog::TxAbort(tx::transaction_id_t tx_id) {
  Emplace({Op::Type::TRANSACTION_ABORT, tx_id});
}

void WriteAheadLog::CreateVertex(tx::transaction_id_t tx_id,
                                 int64_t vertex_id) {
  Op op(Op::Type::CREATE_VERTEX, tx_id);
  op.vertex_id_ = vertex_id;
  Emplace(std::move(op));
}

void WriteAheadLog::CreateEdge(tx::transaction_id_t tx_id, int64_t edge_id,
                               int64_t vertex_from_id, int64_t vertex_to_id,
                               const std::string &edge_type) {
  Op op(Op::Type::CREATE_EDGE, tx_id);
  op.edge_id_ = edge_id;
  op.vertex_from_id_ = vertex_from_id;
  op.vertex_to_id_ = vertex_to_id;
  op.edge_type_ = edge_type;
  Emplace(std::move(op));
}

void WriteAheadLog::PropsSetVertex(tx::transaction_id_t tx_id,
                                   int64_t vertex_id,
                                   const std::string &property,
                                   const PropertyValue &value) {
  Op op(Op::Type::SET_PROPERTY_VERTEX, tx_id);
  op.vertex_id_ = vertex_id;
  op.property_ = property;
  op.value_ = value;
  Emplace(std::move(op));
}

void WriteAheadLog::PropsSetEdge(tx::transaction_id_t tx_id, int64_t edge_id,
                                 const std::string &property,
                                 const PropertyValue &value) {
  Op op(Op::Type::SET_PROPERTY_EDGE, tx_id);
  op.edge_id_ = edge_id;
  op.property_ = property;
  op.value_ = value;
  Emplace(std::move(op));
}

void WriteAheadLog::AddLabel(tx::transaction_id_t tx_id, int64_t vertex_id,
                             const std::string &label) {
  Op op(Op::Type::ADD_LABEL, tx_id);
  op.vertex_id_ = vertex_id;
  op.label_ = label;
  Emplace(std::move(op));
}

void WriteAheadLog::RemoveLabel(tx::transaction_id_t tx_id, int64_t vertex_id,
                                const std::string &label) {
  Op op(Op::Type::REMOVE_LABEL, tx_id);
  op.vertex_id_ = vertex_id;
  op.label_ = label;
  Emplace(std::move(op));
}

void WriteAheadLog::RemoveVertex(tx::transaction_id_t tx_id,
                                 int64_t vertex_id) {
  Op op(Op::Type::REMOVE_VERTEX, tx_id);
  op.vertex_id_ = vertex_id;
  Emplace(std::move(op));
}

void WriteAheadLog::RemoveEdge(tx::transaction_id_t tx_id, int64_t edge_id) {
  Op op(Op::Type::REMOVE_EDGE, tx_id);
  op.edge_id_ = edge_id;
  Emplace(std::move(op));
}

void WriteAheadLog::BuildIndex(tx::transaction_id_t tx_id,
                               const std::string &label,
                               const std::string &property) {
  Op op(Op::Type::BUILD_INDEX, tx_id);
  op.label_ = label;
  op.property_ = property;
  Emplace(std::move(op));
}

WriteAheadLog::WalFile::~WalFile() {
  if (!current_wal_file_.empty()) writer_.Close();
}

namespace {
auto MakeFilePath(const std::string &suffix) {
  return std::experimental::filesystem::path(FLAGS_wal_directory) /
         (Timestamp::now().to_iso8601() + suffix);
}
}

void WriteAheadLog::WalFile::Init() {
  if (!std::experimental::filesystem::exists(FLAGS_wal_directory) &&
      !std::experimental::filesystem::create_directories(FLAGS_wal_directory)) {
    LOG(ERROR) << "Can't write to WAL directory: " << FLAGS_wal_directory;
    current_wal_file_ = std::experimental::filesystem::path();
  } else {
    current_wal_file_ = MakeFilePath("__current");
    try {
      writer_.Open(current_wal_file_);
    } catch (std::ios_base::failure &) {
      LOG(ERROR) << "Failed to open write-ahead log file: "
                 << current_wal_file_;
      current_wal_file_ = std::experimental::filesystem::path();
    }
  }
  latest_tx_ = 0;
  current_wal_file_ops_count_ = 0;
}

void WriteAheadLog::WalFile::Flush(RingBuffer<Op> &buffer) {
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
void WriteAheadLog::WalFile::RotateFile() {
  writer_.Close();
  std::experimental::filesystem::rename(
      current_wal_file_,
      MakeFilePath("__max_transaction_" + std::to_string(latest_tx_)));
  Init();
}

void WriteAheadLog::Emplace(Op &&op) {
  if (enabled_ && FLAGS_wal_flush_interval_millis >= 0)
    ops_.emplace(std::move(op));
}

}  // namespace durability
