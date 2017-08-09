#include "storage/record_accessor.hpp"
#include "database/graph_db_accessor.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "utils/assert.hpp"

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(mvcc::VersionList<TRecord> &vlist,
                                        GraphDbAccessor &db_accessor)
    : db_accessor_(&db_accessor), vlist_(&vlist) {
  Reconstruct();
}

template <typename TRecord>
const PropertyValue &RecordAccessor<TRecord>::PropsAt(
    GraphDbTypes::Property key) const {
  return current().properties_.at(key);
}

template <typename TRecord>
size_t RecordAccessor<TRecord>::PropsErase(GraphDbTypes::Property key) {
  return update().properties_.erase(key);
}

template <typename TRecord>
void RecordAccessor<TRecord>::PropsClear() {
  update().properties_.clear();
}

template <typename TRecord>
const PropertyValueStore<GraphDbTypes::Property>
    &RecordAccessor<TRecord>::Properties() const {
  return current().properties_;
}

template <typename TRecord>
void RecordAccessor<TRecord>::PropertiesAccept(
    std::function<void(const GraphDbTypes::Property key,
                       const PropertyValue &prop)>
        handler,
    std::function<void()> finish) const {
  current().properties_.Accept(handler, finish);
}

template <typename TRecord>
GraphDbAccessor &RecordAccessor<TRecord>::db_accessor() const {
  return *db_accessor_;
}

template <typename TRecord>
uint64_t RecordAccessor<TRecord>::temporary_id() const {
  return (uint64_t)vlist_;
}

template <typename TRecord>
RecordAccessor<TRecord> &RecordAccessor<TRecord>::SwitchNew() {
  if (!new_) {
    // if new_ is not set yet, look for it
    // we can just Reconstruct the pointers, old_ will get initialized
    // to the same value as it has now, and the amount of work is the
    // same as just looking for a new_ record
    if (!Reconstruct())
      debug_fail(
          "RecordAccessor::SwitchNew - accessor invalid after Reconstruct");
  }
  current_ = new_ ? new_ : old_;
  return *this;
}

template <typename TRecord>
RecordAccessor<TRecord> &RecordAccessor<TRecord>::SwitchOld() {
  current_ = old_ ? old_ : new_;
  return *this;
}

template <typename TRecord>
bool RecordAccessor<TRecord>::Reconstruct() {
  return db_accessor().Reconstruct(*this);
}

template <typename TRecord>
TRecord &RecordAccessor<TRecord>::update() {
  db_accessor().Update(*this);
  debug_assert(new_ != nullptr, "RecordAccessor.new_ is null after update");
  return *new_;
}

template <typename TRecord>
const TRecord &RecordAccessor<TRecord>::current() const {
  debug_assert(current_ != nullptr,
               "RecordAccessor.current_ pointer is nullptr");
  return *current_;
}

template <>
void RecordAccessor<Vertex>::PropsSet(GraphDbTypes::Property key,
                                      PropertyValue value) {
  Vertex &vertex = update();
  vertex.properties_.set(key, value);
  this->db_accessor().UpdatePropertyIndex(key, *this, &vertex);
}
template <>
void RecordAccessor<Edge>::PropsSet(GraphDbTypes::Property key,
                                    PropertyValue value) {
  update().properties_.set(key, value);
}

template class RecordAccessor<Vertex>;
template class RecordAccessor<Edge>;
