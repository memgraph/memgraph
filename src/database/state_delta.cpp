#include <string>

#include "communication/bolt/v1/decoder/decoded_value.hpp"
#include "database/graph_db_accessor.hpp"
#include "database/state_delta.hpp"

namespace database {

std::pair<std::string, std::string> StateDelta::IndexName() const {
  CHECK(StateDelta::type() == StateDelta::Type::BUILD_INDEX)
      << "Invalid operation type to try to get index name";
  return std::make_pair(label_, property_);
}

StateDelta StateDelta::TxBegin(tx::transaction_id_t tx_id) {
  return {StateDelta::Type::TRANSACTION_BEGIN, tx_id};
}

StateDelta StateDelta::TxCommit(tx::transaction_id_t tx_id) {
  return {StateDelta::Type::TRANSACTION_COMMIT, tx_id};
}

StateDelta StateDelta::TxAbort(tx::transaction_id_t tx_id) {
  return {StateDelta::Type::TRANSACTION_ABORT, tx_id};
}

StateDelta StateDelta::CreateVertex(tx::transaction_id_t tx_id,
                                    gid::Gid vertex_id) {
  StateDelta op(StateDelta::Type::CREATE_VERTEX, tx_id);
  op.vertex_id_ = vertex_id;
  return op;
}

StateDelta StateDelta::CreateEdge(tx::transaction_id_t tx_id, gid::Gid edge_id,
                                  gid::Gid vertex_from_id,
                                  gid::Gid vertex_to_id,
                                  const std::string &edge_type) {
  StateDelta op(StateDelta::Type::CREATE_EDGE, tx_id);
  op.edge_id_ = edge_id;
  op.vertex_from_id_ = vertex_from_id;
  op.vertex_to_id_ = vertex_to_id;
  op.edge_type_ = edge_type;
  return op;
}

StateDelta StateDelta::PropsSetVertex(tx::transaction_id_t tx_id,
                                      gid::Gid vertex_id,
                                      const std::string &property,
                                      const PropertyValue &value) {
  StateDelta op(StateDelta::Type::SET_PROPERTY_VERTEX, tx_id);
  op.vertex_id_ = vertex_id;
  op.property_ = property;
  op.value_ = value;
  return op;
}

StateDelta StateDelta::PropsSetEdge(tx::transaction_id_t tx_id,
                                    gid::Gid edge_id,
                                    const std::string &property,
                                    const PropertyValue &value) {
  StateDelta op(StateDelta::Type::SET_PROPERTY_EDGE, tx_id);
  op.edge_id_ = edge_id;
  op.property_ = property;
  op.value_ = value;
  return op;
}

StateDelta StateDelta::AddLabel(tx::transaction_id_t tx_id, gid::Gid vertex_id,
                                const std::string &label) {
  StateDelta op(StateDelta::Type::ADD_LABEL, tx_id);
  op.vertex_id_ = vertex_id;
  op.label_ = label;
  return op;
}

StateDelta StateDelta::RemoveLabel(tx::transaction_id_t tx_id,
                                   gid::Gid vertex_id,
                                   const std::string &label) {
  StateDelta op(StateDelta::Type::REMOVE_LABEL, tx_id);
  op.vertex_id_ = vertex_id;
  op.label_ = label;
  return op;
}

StateDelta StateDelta::RemoveVertex(tx::transaction_id_t tx_id,
                                    gid::Gid vertex_id) {
  StateDelta op(StateDelta::Type::REMOVE_VERTEX, tx_id);
  op.vertex_id_ = vertex_id;
  return op;
}

StateDelta StateDelta::RemoveEdge(tx::transaction_id_t tx_id,
                                  gid::Gid edge_id) {
  StateDelta op(StateDelta::Type::REMOVE_EDGE, tx_id);
  op.edge_id_ = edge_id;
  return op;
}

StateDelta StateDelta::BuildIndex(tx::transaction_id_t tx_id,
                                  const std::string &label,
                                  const std::string &property) {
  StateDelta op(StateDelta::Type::BUILD_INDEX, tx_id);
  op.label_ = label;
  op.property_ = property;
  return op;
}

void StateDelta::Encode(
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

std::experimental::optional<StateDelta> StateDelta::Decode(
    HashedFileReader &reader,
    communication::bolt::Decoder<HashedFileReader> &decoder) {
  using std::experimental::nullopt;

  StateDelta r_val;
  // The decoded value used as a temporary while decoding.
  communication::bolt::DecodedValue dv;

  try {
    if (!decoder.ReadValue(&dv)) return nullopt;
    r_val.type_ = static_cast<enum StateDelta::Type>(dv.ValueInt());
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

void StateDelta::Apply(GraphDbAccessor &dba) const {
  switch (type_) {
    // Transactional state is not recovered.
    case Type::TRANSACTION_BEGIN:
    case Type::TRANSACTION_COMMIT:
    case Type::TRANSACTION_ABORT:
      LOG(FATAL) << "Transaction handling not handled in Apply";
      break;
    case Type::CREATE_VERTEX:
      dba.InsertVertex(vertex_id_);
      break;
    case Type::CREATE_EDGE: {
      auto from = dba.FindVertex(vertex_from_id_, true);
      auto to = dba.FindVertex(vertex_to_id_, true);
      DCHECK(from) << "Failed to find vertex.";
      DCHECK(to) << "Failed to find vertex.";
      dba.InsertEdge(*from, *to, dba.EdgeType(edge_type_), edge_id_);
      break;
    }
    case Type::SET_PROPERTY_VERTEX: {
      auto vertex = dba.FindVertex(vertex_id_, true);
      DCHECK(vertex) << "Failed to find vertex.";
      vertex->PropsSet(dba.Property(property_), value_);
      break;
    }
    case Type::SET_PROPERTY_EDGE: {
      auto edge = dba.FindEdge(edge_id_, true);
      DCHECK(edge) << "Failed to find edge.";
      edge->PropsSet(dba.Property(property_), value_);
      break;
    }
    case Type::ADD_LABEL: {
      auto vertex = dba.FindVertex(vertex_id_, true);
      DCHECK(vertex) << "Failed to find vertex.";
      vertex->add_label(dba.Label(label_));
      break;
    }
    case Type::REMOVE_LABEL: {
      auto vertex = dba.FindVertex(vertex_id_, true);
      DCHECK(vertex) << "Failed to find vertex.";
      vertex->remove_label(dba.Label(label_));
      break;
    }
    case Type::REMOVE_VERTEX: {
      auto vertex = dba.FindVertex(vertex_id_, true);
      DCHECK(vertex) << "Failed to find vertex.";
      dba.DetachRemoveVertex(*vertex);
      break;
    }
    case Type::REMOVE_EDGE: {
      auto edge = dba.FindEdge(edge_id_, true);
      DCHECK(edge) << "Failed to find edge.";
      dba.RemoveEdge(*edge);
      break;
    }
    case Type::BUILD_INDEX: {
      LOG(FATAL) << "Index handling not handled in Apply";
      break;
    }
  }
}

};  // namespace database
