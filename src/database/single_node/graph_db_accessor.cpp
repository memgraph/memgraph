#include "database/single_node/graph_db_accessor.hpp"

#include <chrono>
#include <thread>

#include <glog/logging.h>

#include "durability/single_node/state_delta.hpp"
#include "storage/common/constraints/exceptions.hpp"
#include "storage/single_node/edge.hpp"
#include "storage/single_node/edge_accessor.hpp"
#include "storage/single_node/vertex.hpp"
#include "storage/single_node/vertex_accessor.hpp"
#include "utils/cast.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/stat.hpp"

namespace database {

GraphDbAccessor::GraphDbAccessor(GraphDb *db)
    : db_(db),
      transaction_(db->tx_engine().Begin()),
      transaction_starter_{true} {}

GraphDbAccessor::GraphDbAccessor(GraphDb *db, tx::TransactionId tx_id)
    : db_(db),
      transaction_(db->tx_engine().RunningTransaction(tx_id)),
      transaction_starter_{false} {}

GraphDbAccessor::GraphDbAccessor(GraphDb *db,
                                 std::optional<tx::TransactionId> parent_tx)
    : db_(db),
      transaction_(db->tx_engine().BeginBlocking(parent_tx)),
      transaction_starter_{true} {}

GraphDbAccessor::GraphDbAccessor(GraphDbAccessor &&other) noexcept
    : db_(other.db_),
      transaction_(other.transaction_),
      transaction_starter_(other.transaction_starter_),
      commited_(other.commited_),
      aborted_(other.aborted_) {
  // Make sure that the other transaction isn't a transaction starter so that
  // its destructor doesn't close the transaction.
  other.transaction_starter_ = false;
}

GraphDbAccessor &GraphDbAccessor::operator=(GraphDbAccessor &&other) noexcept {
  db_ = other.db_;
  transaction_ = other.transaction_;
  transaction_starter_ = other.transaction_starter_;
  commited_ = other.commited_;
  aborted_ = other.aborted_;

  // Make sure that the other transaction isn't a transaction starter so that
  // its destructor doesn't close the transaction.
  other.transaction_starter_ = false;

  return *this;
}

GraphDbAccessor::~GraphDbAccessor() {
  if (transaction_starter_ && !commited_ && !aborted_) {
    this->Abort();
  }
}

tx::TransactionId GraphDbAccessor::transaction_id() const {
  return transaction_->id_;
}

void GraphDbAccessor::AdvanceCommand() {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  db_->tx_engine().Advance(transaction_->id_);
}

void GraphDbAccessor::Commit() {
  DCHECK(!commited_ && !aborted_) << "Already aborted or commited transaction.";
  db_->tx_engine().Commit(*transaction_);
  commited_ = true;
}

void GraphDbAccessor::Abort() {
  DCHECK(!commited_ && !aborted_) << "Already aborted or commited transaction.";
  db_->tx_engine().Abort(*transaction_);
  aborted_ = true;
}

bool GraphDbAccessor::should_abort() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return transaction_->should_abort();
}

durability::WriteAheadLog &GraphDbAccessor::wal() { return db_->wal(); }

VertexAccessor GraphDbAccessor::InsertVertex(
    std::optional<gid::Gid> requested_gid) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  auto gid = db_->storage().vertex_generator_.Next(requested_gid);
  auto vertex_vlist = new mvcc::VersionList<Vertex>(*transaction_, gid);

  bool success =
      db_->storage().vertices_.access().insert(gid, vertex_vlist).second;
  CHECK(success) << "Attempting to insert a vertex with an existing GID: "
                 << gid;
  wal().Emplace(
      database::StateDelta::CreateVertex(transaction_->id_, vertex_vlist->gid_));
  auto va = VertexAccessor(vertex_vlist, *this);
  return va;
}

std::optional<VertexAccessor> GraphDbAccessor::FindVertexOptional(
    gid::Gid gid, bool current_state) {
  VertexAccessor record_accessor(db_->storage().LocalAddress<Vertex>(gid),
                                 *this);
  if (!record_accessor.Visible(transaction(), current_state))
    return std::nullopt;
  return record_accessor;
}

VertexAccessor GraphDbAccessor::FindVertex(gid::Gid gid, bool current_state) {
  auto found = FindVertexOptional(gid, current_state);
  CHECK(found) << "Unable to find vertex for id: " << gid;
  return *found;
}

std::optional<EdgeAccessor> GraphDbAccessor::FindEdgeOptional(
    gid::Gid gid, bool current_state) {
  EdgeAccessor record_accessor(db_->storage().LocalAddress<Edge>(gid), *this);
  if (!record_accessor.Visible(transaction(), current_state))
    return std::nullopt;
  return record_accessor;
}

EdgeAccessor GraphDbAccessor::FindEdge(gid::Gid gid, bool current_state) {
  auto found = FindEdgeOptional(gid, current_state);
  CHECK(found) << "Unable to find edge for id: " << gid;
  return *found;
}

void GraphDbAccessor::BuildIndex(storage::Label label,
                                 storage::Property property) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  // Create the index
  const LabelPropertyIndex::Key key(label, property);
  if (db_->storage().label_property_index_.CreateIndex(key) == false) {
    throw IndexExistsException(
        "Index is either being created by another transaction or already "
        "exists.");
  }

  try {
    auto dba = db_->AccessBlocking(std::make_optional(transaction_->id_));

    dba.PopulateIndex(key);
    dba.EnableIndex(key);
    dba.Commit();
  } catch (const tx::TransactionEngineError &e) {
    db_->storage().label_property_index_.DeleteIndex(key);
    throw TransactionException(e.what());
  }
}

void GraphDbAccessor::EnableIndex(const LabelPropertyIndex::Key &key) {
  // Commit transaction as we finished applying method on newest visible
  // records. Write that transaction's ID to the WAL as the index has been
  // built at this point even if this DBA's transaction aborts for some
  // reason.
  wal().Emplace(database::StateDelta::BuildIndex(
      transaction_id(), key.label_, LabelName(key.label_), key.property_,
      PropertyName(key.property_)));
}

void GraphDbAccessor::PopulateIndex(const LabelPropertyIndex::Key &key) {
  for (auto vertex : Vertices(key.label_, false)) {
    if (vertex.PropsAt(key.property_).type() == PropertyValue::Type::Null)
      continue;
    db_->storage().label_property_index_.UpdateOnLabelProperty(vertex.address(),
                                                               vertex.current_);
  }
}

void GraphDbAccessor::DeleteIndex(storage::Label label,
                                  storage::Property property) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  LabelPropertyIndex::Key key(label, property);
  try {
    auto dba = db_->AccessBlocking(std::make_optional(transaction_->id_));

    db_->storage().label_property_index_.DeleteIndex(key);
    dba.wal().Emplace(database::StateDelta::DropIndex(
        dba.transaction_id(), key.label_, LabelName(key.label_), key.property_,
        PropertyName(key.property_)));

    dba.Commit();
  } catch (const tx::TransactionEngineError &e) {
    throw TransactionException(e.what());
  }
}

void GraphDbAccessor::BuildUniqueConstraint(
    storage::Label label, const std::vector<storage::Property> &properties) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  storage::constraints::ConstraintEntry entry{label, properties};
  if (!db_->storage().unique_constraints_.AddConstraint(entry)) {
    // Already exists
    return;
  }

  try {
    auto dba = db_->AccessBlocking(std::make_optional(transaction().id_));

    for (auto v : dba.Vertices(false)) {
      if (std::find(v.labels().begin(), v.labels().end(), label) !=
          v.labels().end()) {
        db_->storage().unique_constraints_.Update(v, dba.transaction());
      }
    }

    std::vector<std::string> property_names(properties.size());
    std::transform(properties.begin(), properties.end(), property_names.begin(),
                   [&dba](storage::Property property) {
                     return dba.PropertyName(property);
                   });

    dba.wal().Emplace(database::StateDelta::BuildUniqueConstraint(
        dba.transaction().id_, label, dba.LabelName(label), properties,
        property_names));

    dba.Commit();

  } catch (const tx::TransactionEngineError &e) {
    db_->storage().unique_constraints_.RemoveConstraint(entry);
    throw TransactionException(e.what());
  } catch (const storage::constraints::ViolationException &e) {
    db_->storage().unique_constraints_.RemoveConstraint(entry);
    throw ConstraintViolationException(e.what());
  } catch (const storage::constraints::SerializationException &e) {
    db_->storage().unique_constraints_.RemoveConstraint(entry);
    throw mvcc::SerializationError();
  } catch (...) {
    db_->storage().unique_constraints_.RemoveConstraint(entry);
    throw;
  }
}

void GraphDbAccessor::DeleteUniqueConstraint(
    storage::Label label, const std::vector<storage::Property> &properties) {
  storage::constraints::ConstraintEntry entry{label, properties};
  try {
    auto dba = db_->AccessBlocking(std::make_optional(transaction().id_));

    if (!db_->storage().unique_constraints_.RemoveConstraint(entry)) {
      // Nothing was deleted

      return;
    }

    std::vector<std::string> property_names(properties.size());
    std::transform(properties.begin(), properties.end(), property_names.begin(),
                   [&dba](storage::Property property) {
                     return dba.PropertyName(property);
                   });

    dba.wal().Emplace(database::StateDelta::DropUniqueConstraint(
        dba.transaction().id_, label, dba.LabelName(label), properties,
        property_names));

    dba.Commit();
  } catch (const tx::TransactionEngineError &e) {
    throw TransactionException(e.what());
  }
}

std::vector<storage::constraints::ConstraintEntry>
GraphDbAccessor::ListUniqueConstraints() const {
  return db_->storage().unique_constraints_.ListConstraints();
}

void GraphDbAccessor::UpdateOnAddLabel(storage::Label label,
                                       const VertexAccessor &vertex_accessor,
                                       const Vertex *vertex) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  auto *vlist_ptr = vertex_accessor.address();

  try {
    db_->storage().unique_constraints_.UpdateOnAddLabel(label, vertex_accessor,
                                                        transaction());
  } catch (const storage::constraints::SerializationException &e) {
    throw mvcc::SerializationError();
  } catch (const storage::constraints::ViolationException &e) {
    throw ConstraintViolationException(e.what());
  }

  db_->storage().label_property_index_.UpdateOnLabel(label, vlist_ptr, vertex);
  db_->storage().labels_index_.Update(label, vlist_ptr, vertex);
}

void GraphDbAccessor::UpdateOnRemoveLabel(
    storage::Label label, const RecordAccessor<Vertex> &accessor) {
  db_->storage().unique_constraints_.UpdateOnRemoveLabel(label, accessor,
                                                         transaction());
}

void GraphDbAccessor::UpdateOnAddProperty(
    storage::Property property, const PropertyValue &previous_value,
    const PropertyValue &new_value,
    const RecordAccessor<Vertex> &vertex_accessor, const Vertex *vertex) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  try {
    db_->storage().unique_constraints_.UpdateOnAddProperty(
        property, previous_value, new_value, vertex_accessor, transaction());
  } catch (const storage::constraints::SerializationException &e) {
    throw mvcc::SerializationError();
  } catch (const storage::constraints::ViolationException &e) {
    throw ConstraintViolationException(e.what());
  }

  db_->storage().label_property_index_.UpdateOnProperty(
      property, vertex_accessor.address(), vertex);
}

void GraphDbAccessor::UpdateOnRemoveProperty(
    storage::Property property, const PropertyValue &previous_value,
    const RecordAccessor<Vertex> &accessor, const Vertex *vertex) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  try {
    db_->storage().unique_constraints_.UpdateOnRemoveProperty(
        property, previous_value, accessor, transaction());
  } catch (const storage::constraints::SerializationException &e) {
    throw mvcc::SerializationError();
  }
}

int64_t GraphDbAccessor::VerticesCount() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_->storage().vertices_.access().size();
}

int64_t GraphDbAccessor::VerticesCount(storage::Label label) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_->storage().labels_index_.Count(label);
}

int64_t GraphDbAccessor::VerticesCount(storage::Label label,
                                       storage::Property property) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  const LabelPropertyIndex::Key key(label, property);
  DCHECK(db_->storage().label_property_index_.IndexExists(key))
      << "Index doesn't exist.";
  return db_->storage().label_property_index_.Count(key);
}

int64_t GraphDbAccessor::VerticesCount(storage::Label label,
                                       storage::Property property,
                                       const PropertyValue &value) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  const LabelPropertyIndex::Key key(label, property);
  DCHECK(db_->storage().label_property_index_.IndexExists(key))
      << "Index doesn't exist.";
  return db_->storage()
      .label_property_index_.PositionAndCount(key, value)
      .second;
}

int64_t GraphDbAccessor::VerticesCount(
    storage::Label label, storage::Property property,
    const std::optional<utils::Bound<PropertyValue>> lower,
    const std::optional<utils::Bound<PropertyValue>> upper) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  const LabelPropertyIndex::Key key(label, property);
  DCHECK(db_->storage().label_property_index_.IndexExists(key))
      << "Index doesn't exist.";
  CHECK(lower || upper) << "At least one bound must be provided";
  CHECK(!lower || lower.value().value().type() != PropertyValue::Type::Null)
      << "Null value is not a valid index bound";
  CHECK(!upper || upper.value().value().type() != PropertyValue::Type::Null)
      << "Null value is not a valid index bound";

  if (!upper) {
    auto lower_pac = db_->storage().label_property_index_.PositionAndCount(
        key, lower.value().value());
    int64_t size = db_->storage().label_property_index_.Count(key);
    return std::max(0l,
                    size - lower_pac.first -
                        (lower.value().IsInclusive() ? 0l : lower_pac.second));

  } else if (!lower) {
    auto upper_pac = db_->storage().label_property_index_.PositionAndCount(
        key, upper.value().value());
    return upper.value().IsInclusive() ? upper_pac.first + upper_pac.second
                                       : upper_pac.first;

  } else {
    auto lower_pac = db_->storage().label_property_index_.PositionAndCount(
        key, lower.value().value());
    auto upper_pac = db_->storage().label_property_index_.PositionAndCount(
        key, upper.value().value());
    auto result = upper_pac.first - lower_pac.first;
    if (lower.value().IsExclusive()) result -= lower_pac.second;
    if (upper.value().IsInclusive()) result += upper_pac.second;
    return std::max(0l, result);
  }
}

bool GraphDbAccessor::RemoveVertex(VertexAccessor &vertex_accessor,
                                   bool check_empty) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  vertex_accessor.SwitchNew();
  // it's possible the vertex was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  if (vertex_accessor.current().is_expired_by(*transaction_)) return true;
  if (check_empty &&
      vertex_accessor.out_degree() + vertex_accessor.in_degree() > 0)
    return false;

  // Notify unique constraints that vertex_accessor has been deleted
  db_->storage().unique_constraints_.UpdateOnRemoveVertex(vertex_accessor,
                                                          transaction());

  auto *vlist_ptr = vertex_accessor.address();
  wal().Emplace(database::StateDelta::RemoveVertex(
      transaction_->id_, vlist_ptr->gid_, check_empty));
  vlist_ptr->remove(vertex_accessor.current_, *transaction_);
  return true;
}

void GraphDbAccessor::DetachRemoveVertex(VertexAccessor &vertex_accessor) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  vertex_accessor.SwitchNew();

  // Note that when we call RemoveEdge we must take care not to delete from the
  // collection we are iterating over. This invalidates the iterator in a subtle
  // way that does not fail in tests, but is NOT correct.
  for (auto edge_accessor : vertex_accessor.in())
    RemoveEdge(edge_accessor, true, false);
  vertex_accessor.SwitchNew();
  for (auto edge_accessor : vertex_accessor.out())
    RemoveEdge(edge_accessor, false, true);

  RemoveVertex(vertex_accessor, false);
}

EdgeAccessor GraphDbAccessor::InsertEdge(
    VertexAccessor &from, VertexAccessor &to, storage::EdgeType edge_type,
    std::optional<gid::Gid> requested_gid) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  auto gid = db_->storage().edge_generator_.Next(requested_gid);
  auto edge_vlist = new mvcc::VersionList<Edge>(
      *transaction_, gid, from.address(), to.address(), edge_type);
  // We need to insert edge_vlist to edges_ before calling update since update
  // can throw and edge_vlist will not be garbage collected if it is not in
  // edges_ skiplist.
  bool success = db_->storage().edges_.access().insert(gid, edge_vlist).second;
  CHECK(success) << "Attempting to insert an edge with an existing GID: "
                 << gid;

  // ensure that the "from" accessor has the latest version
  from.SwitchNew();
  from.update().out_.emplace(to.address(), edge_vlist, edge_type);

  // ensure that the "to" accessor has the latest version (Switch new)
  // WARNING: must do that after the above "from.update()" for cases when
  // we are creating a cycle and "from" and "to" are the same vlist
  to.SwitchNew();
  to.update().in_.emplace(from.address(), edge_vlist, edge_type);

  wal().Emplace(database::StateDelta::CreateEdge(
      transaction_->id_, edge_vlist->gid_, from.gid(), to.gid(), edge_type,
      EdgeTypeName(edge_type)));

  return EdgeAccessor(edge_vlist, *this, from.address(), to.address(),
                      edge_type);
}

int64_t GraphDbAccessor::EdgesCount() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_->storage().edges_.access().size();
}

void GraphDbAccessor::RemoveEdge(EdgeAccessor &edge, bool remove_out_edge,
                                 bool remove_in_edge) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  // it's possible the edge was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  edge.SwitchNew();
  if (edge.current().is_expired_by(*transaction_)) return;
  if (remove_out_edge) edge.from().RemoveOutEdge(edge.address());
  if (remove_in_edge) edge.to().RemoveInEdge(edge.address());

  edge.address()->remove(edge.current_, *transaction_);
  wal().Emplace(database::StateDelta::RemoveEdge(transaction_->id_, edge.gid()));
}

storage::Label GraphDbAccessor::Label(const std::string &label_name) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_->label_mapper().value_to_id(label_name);
}

const std::string &GraphDbAccessor::LabelName(storage::Label label) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_->label_mapper().id_to_value(label);
}

storage::EdgeType GraphDbAccessor::EdgeType(const std::string &edge_type_name) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_->edge_type_mapper().value_to_id(edge_type_name);
}

const std::string &GraphDbAccessor::EdgeTypeName(
    storage::EdgeType edge_type) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_->edge_type_mapper().id_to_value(edge_type);
}

storage::Property GraphDbAccessor::Property(const std::string &property_name) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_->property_mapper().value_to_id(property_name);
}

const std::string &GraphDbAccessor::PropertyName(
    storage::Property property) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_->property_mapper().id_to_value(property);
}

std::vector<std::string> GraphDbAccessor::IndexInfo() const {
  std::vector<std::string> info;
  for (storage::Label label : db_->storage().labels_index_.Keys()) {
    info.emplace_back(":" + LabelName(label));
  }
  for (LabelPropertyIndex::Key key :
       db_->storage().label_property_index_.Keys()) {
    info.emplace_back(fmt::format(":{}({})", LabelName(key.label_),
                                  PropertyName(key.property_)));
  }
  return info;
}

std::vector<std::pair<std::string, std::string>> GraphDbAccessor::StorageInfo()
    const {
  std::vector<std::pair<std::string, std::string>> info;

  db_->RefreshStat();
  auto &stat = db_->GetStat();

  info.emplace_back("vertex_count", std::to_string(stat.vertex_count));
  info.emplace_back("edge_count", std::to_string(stat.edge_count));
  info.emplace_back("average_degree", std::to_string(stat.avg_degree));
  info.emplace_back("memory_usage", std::to_string(utils::GetMemoryUsage()));
  info.emplace_back("disk_usage", std::to_string(db_->GetDurabilityDirDiskUsage()));

  return info;
}

}  // namespace database
