#pragma once

#include "communication/bolt/v1/decoder/decoder.hpp"
#include "communication/bolt/v1/encoder/primitive_encoder.hpp"
#include "durability/hashed_file_reader.hpp"
#include "durability/hashed_file_writer.hpp"
#include "storage/gid.hpp"
#include "storage/property_value.hpp"

namespace database {
/** Describes single change to the database state. Used for durability (WAL) and
 * state communication over network in HA and for distributed remote storage
 * changes*/
class StateDelta {
 public:
  /** Defines StateDelta type. For each type the comment indicates which values
   * need to be stored. All deltas have the transaction_id member, so that's
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

  StateDelta() = default;
  StateDelta(const enum Type &type, tx::transaction_id_t tx_id)
      : type_(type), transaction_id_(tx_id) {}

  /** Attempts to decode a StateDelta from the given decoder. Returns the
   * decoded value if successful, otherwise returns nullopt. */
  static std::experimental::optional<StateDelta> Decode(
      HashedFileReader &reader,
      communication::bolt::Decoder<HashedFileReader> &decoder);

  /** Encodes the delta using primitive encoder, and writes out the new hash
   * with delta to the writer */
  void Encode(
      HashedFileWriter &writer,
      communication::bolt::PrimitiveEncoder<HashedFileWriter> &encoder) const;

  tx::transaction_id_t transaction_id() const { return transaction_id_; }
  Type type() const { return type_; }

  static StateDelta TxBegin(tx::transaction_id_t tx_id);
  static StateDelta TxCommit(tx::transaction_id_t tx_id);
  static StateDelta TxAbort(tx::transaction_id_t tx_id);
  static StateDelta CreateVertex(tx::transaction_id_t tx_id,
                                 gid::Gid vertex_id);
  static StateDelta CreateEdge(tx::transaction_id_t tx_id, gid::Gid edge_id,
                               gid::Gid vertex_from_id, gid::Gid vertex_to_id,
                               const std::string &edge_type);
  static StateDelta PropsSetVertex(tx::transaction_id_t tx_id,
                                   gid::Gid vertex_id,
                                   const std::string &property,
                                   const PropertyValue &value);
  static StateDelta PropsSetEdge(tx::transaction_id_t tx_id, gid::Gid edge_id,
                                 const std::string &property,
                                 const PropertyValue &value);
  static StateDelta AddLabel(tx::transaction_id_t tx_id, gid::Gid vertex_id,
                             const std::string &label);
  static StateDelta RemoveLabel(tx::transaction_id_t tx_id, gid::Gid vertex_id,
                                const std::string &label);
  static StateDelta RemoveVertex(tx::transaction_id_t tx_id,
                                 gid::Gid vertex_id);
  static StateDelta RemoveEdge(tx::transaction_id_t tx_id, gid::Gid edge_id);
  static StateDelta BuildIndex(tx::transaction_id_t tx_id,
                               const std::string &label,
                               const std::string &property);

  std::pair<std::string, std::string> IndexName() const;

  /// Applies CRUD delta to database accessor. Fails on other types of deltas
  void Apply(GraphDbAccessor &dba) const;

 private:
  // Members valid for every delta.
  enum Type type_;
  tx::transaction_id_t transaction_id_;

  // Members valid only for some deltas, see StateDelta::Type comments above.
  gid::Gid vertex_id_;
  gid::Gid edge_id_;
  gid::Gid vertex_from_id_;
  gid::Gid vertex_to_id_;
  std::string edge_type_;
  std::string property_;
  PropertyValue value_ = PropertyValue::Null;
  std::string label_;
};
}  // namespace database
