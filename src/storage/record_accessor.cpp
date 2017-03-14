#include "storage/record_accessor.hpp"
#include "database/graph_db_accessor.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "utils/assert.hpp"

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(mvcc::VersionList<TRecord> &vlist,
                                        GraphDbAccessor &db_accessor)
    : db_accessor_(&db_accessor), vlist_(&vlist), record_(nullptr) {
  db_accessor.init_record(*this);
  debug_assert(record_ != nullptr, "Record is nullptr.");
}

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(mvcc::VersionList<TRecord> &vlist,
                                        TRecord &record,
                                        GraphDbAccessor &db_accessor)
    : db_accessor_(&db_accessor), vlist_(&vlist), record_(&record) {
  debug_assert(record_ != nullptr, "Record is nullptr.");
}

template <typename TRecord>
const PropertyValue &RecordAccessor<TRecord>::PropsAt(
    GraphDb::Property key) const {
  return view().properties_.at(key);
}

template <typename TRecord>
size_t RecordAccessor<TRecord>::PropsErase(GraphDb::Property key) {
  return update().properties_.erase(key);
}

template <typename TRecord>
const PropertyValueStore<GraphDb::Property>
    &RecordAccessor<TRecord>::Properties() const {
  return view().properties_;
}

template <typename TRecord>
void RecordAccessor<TRecord>::PropertiesAccept(
    std::function<void(const GraphDb::Property key, const PropertyValue &prop)>
        handler,
    std::function<void()> finish) const {
  view().properties_.Accept(handler, finish);
}

template <typename TRecord>
GraphDbAccessor &RecordAccessor<TRecord>::db_accessor() const {
  return *db_accessor_;
}

template <typename TRecord>
TRecord &RecordAccessor<TRecord>::update() {
  db_accessor().update(*this);
  return *record_;
}

template <typename TRecord>
const TRecord &RecordAccessor<TRecord>::view() const {
  return *record_;
}

template class RecordAccessor<Vertex>;
template class RecordAccessor<Edge>;
