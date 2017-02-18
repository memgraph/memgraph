#include "storage/record_accessor.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(mvcc::VersionList<TRecord> &vlist,
                                        GraphDbAccessor &db_accessor)
    : vlist_(vlist),
      record_(vlist_.find(db_accessor.transaction_)),
      db_accessor_(db_accessor) {
  assert(record_ != nullptr);
}

template <typename TRecord>
RecordAccessor<TRecord>::RecordAccessor(mvcc::VersionList<TRecord> &vlist,
                                        TRecord &record,
                                        GraphDbAccessor &db_accessor)
    : vlist_(vlist), record_(&record), db_accessor_(db_accessor) {
  assert(record_ != nullptr);
}

template <typename TRecord>
const TypedValue &RecordAccessor<TRecord>::PropsAt(
    GraphDb::Property key) const {
  return view().properties_.at(key);
}

template <typename TRecord>
size_t RecordAccessor<TRecord>::PropsErase(GraphDb::Property key) {
  return update().properties_.erase(key);
}

template <typename TRecord>
const TypedValueStore<GraphDb::Property> &RecordAccessor<TRecord>::Properties()
    const {
  return view().properties_;
}

template <typename TRecord>
void RecordAccessor<TRecord>::PropertiesAccept(
    std::function<void(const GraphDb::Property key, const TypedValue &prop)>
        handler,
    std::function<void()> finish) const {
  view().properties_.Accept(handler, finish);
}

template <typename TRecord>
GraphDbAccessor &RecordAccessor<TRecord>::db_accessor() {
  return db_accessor_;
}

template <typename TRecord>
const GraphDbAccessor &RecordAccessor<TRecord>::db_accessor() const {
  return db_accessor_;
}

template <typename TRecord>
TRecord &RecordAccessor<TRecord>::update() {
  if (!record_->is_visible_write(db_accessor_.transaction_))
    record_ = vlist_.update(db_accessor_.transaction_);

  return *record_;
}

template <typename TRecord>
const TRecord &RecordAccessor<TRecord>::view() const {
  return *record_;
}

template class RecordAccessor<Vertex>;
template class RecordAccessor<Edge>;
