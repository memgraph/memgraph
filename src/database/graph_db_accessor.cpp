#include "database/creation_exception.hpp"
#include "database/graph_db_accessor.hpp"

#include "storage/edge.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/on_scope_exit.hpp"

GraphDbAccessor::GraphDbAccessor(GraphDb &db)
    : db_(db), transaction_(db.tx_engine_.Begin()) {}

GraphDbAccessor::~GraphDbAccessor() {
  if (!commited_ && !aborted_) {
    this->Abort();
  }
}

void GraphDbAccessor::AdvanceCommand() {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  transaction_->engine_.Advance(transaction_->id_);
}

void GraphDbAccessor::Commit() {
  DCHECK(!commited_ && !aborted_) << "Already aborted or commited transaction.";
  transaction_->Commit();
  commited_ = true;
}

void GraphDbAccessor::Abort() {
  DCHECK(!commited_ && !aborted_) << "Already aborted or commited transaction.";
  transaction_->Abort();
  aborted_ = true;
}

bool GraphDbAccessor::should_abort() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return transaction_->should_abort();
}

VertexAccessor GraphDbAccessor::InsertVertex() {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  // create a vertex
  auto vertex_vlist = new mvcc::VersionList<Vertex>(*transaction_);

  bool success = db_.vertices_.access().insert(vertex_vlist).second;
  CHECK(success) << "It is impossible for new version list to already exist";
  return VertexAccessor(*vertex_vlist, *this);
}

void GraphDbAccessor::BuildIndex(const GraphDbTypes::Label &label,
                                 const GraphDbTypes::Property &property) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

  {
    // switch the build_in_progress to true
    bool expected = false;
    if (!db_.index_build_in_progress_.compare_exchange_strong(expected, true))
      throw IndexBuildInProgressException();
  }

  // on function exit switch the build_in_progress to false
  utils::OnScopeExit on_exit([this] {
    bool expected = true;
    [[gnu::unused]] bool success =
        db_.index_build_in_progress_.compare_exchange_strong(expected, false);
    DCHECK(success) << "BuildIndexInProgress flag was not set during build";
  });

  const LabelPropertyIndex::Key key(label, property);
  if (db_.label_property_index_.CreateIndex(key) == false) {
    throw IndexExistsException(
        "Index is either being created by another transaction or already "
        "exists.");
  }

  // Everything that happens after the line above ended will be added to the
  // index automatically, but we still have to add to index everything that
  // happened earlier. We have to first wait for every transaction that
  // happend before, or a bit later than CreateIndex to end.
  {
    auto wait_transaction = db_.tx_engine_.Begin();
    for (auto id : wait_transaction->snapshot()) {
      if (id == transaction_->id_) continue;
      while (wait_transaction->engine_.clog().is_active(id))
        // TODO reconsider this constant, currently rule-of-thumb chosen
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    wait_transaction->Commit();
  }

  // This accessor's transaction surely sees everything that happened before
  // CreateIndex.
  GraphDbAccessor dba(db_);
  for (auto vertex : dba.Vertices(label, false)) {
    db_.label_property_index_.UpdateOnLabelProperty(vertex.vlist_,
                                                    vertex.current_);
  }
  // Commit transaction as we finished applying method on newest visible
  // records.
  dba.Commit();

  // After these two operations we are certain that everything is contained in
  // the index under the assumption that this transaction contained no
  // vertex/edge insert/update before this method was invoked.
  db_.label_property_index_.IndexFinishedBuilding(key);
}

void GraphDbAccessor::UpdateLabelIndices(const GraphDbTypes::Label &label,
                                         const VertexAccessor &vertex_accessor,
                                         const Vertex *const vertex) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  db_.labels_index_.Update(label, vertex_accessor.vlist_, vertex);
  db_.label_property_index_.UpdateOnLabel(label, vertex_accessor.vlist_,
                                          vertex);
}

void GraphDbAccessor::UpdatePropertyIndex(
    const GraphDbTypes::Property &property,
    const RecordAccessor<Vertex> &record_accessor, const Vertex *const vertex) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  db_.label_property_index_.UpdateOnProperty(property, record_accessor.vlist_,
                                             vertex);
}

int64_t GraphDbAccessor::VerticesCount() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.vertices_.access().size();
}

int64_t GraphDbAccessor::VerticesCount(const GraphDbTypes::Label &label) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.labels_index_.Count(label);
}

int64_t GraphDbAccessor::VerticesCount(
    const GraphDbTypes::Label &label,
    const GraphDbTypes::Property &property) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  const LabelPropertyIndex::Key key(label, property);
  DCHECK(db_.label_property_index_.IndexExists(key)) << "Index doesn't exist.";
  return db_.label_property_index_.Count(key);
}

int64_t GraphDbAccessor::VerticesCount(const GraphDbTypes::Label &label,
                                       const GraphDbTypes::Property &property,
                                       const PropertyValue &value) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  const LabelPropertyIndex::Key key(label, property);
  DCHECK(db_.label_property_index_.IndexExists(key)) << "Index doesn't exist.";
  return db_.label_property_index_.PositionAndCount(key, value).second;
}

int64_t GraphDbAccessor::VerticesCount(
    const GraphDbTypes::Label &label, const GraphDbTypes::Property &property,
    const std::experimental::optional<utils::Bound<PropertyValue>> lower,
    const std::experimental::optional<utils::Bound<PropertyValue>> upper)
    const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  const LabelPropertyIndex::Key key(label, property);
  DCHECK(db_.label_property_index_.IndexExists(key)) << "Index doesn't exist.";
  CHECK(lower || upper) << "At least one bound must be provided";
  CHECK(!lower || lower.value().value().type() != PropertyValue::Type::Null)
      << "Null value is not a valid index bound";
  CHECK(!upper || upper.value().value().type() != PropertyValue::Type::Null)
      << "Null value is not a valid index bound";

  if (!upper) {
    auto lower_pac =
        db_.label_property_index_.PositionAndCount(key, lower.value().value());
    int64_t size = db_.label_property_index_.Count(key);
    return std::max(0l,
                    size - lower_pac.first -
                        (lower.value().IsInclusive() ? 0l : lower_pac.second));

  } else if (!lower) {
    auto upper_pac =
        db_.label_property_index_.PositionAndCount(key, upper.value().value());
    return upper.value().IsInclusive() ? upper_pac.first + upper_pac.second
                                       : upper_pac.first;

  } else {
    auto lower_pac =
        db_.label_property_index_.PositionAndCount(key, lower.value().value());
    auto upper_pac =
        db_.label_property_index_.PositionAndCount(key, upper.value().value());
    auto result = upper_pac.first - lower_pac.first;
    if (lower.value().IsExclusive()) result -= lower_pac.second;
    if (upper.value().IsInclusive()) result += upper_pac.second;
    return std::max(0l, result);
  }
}

bool GraphDbAccessor::RemoveVertex(VertexAccessor &vertex_accessor) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  vertex_accessor.SwitchNew();
  // it's possible the vertex was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  if (vertex_accessor.current_->is_expired_by(*transaction_)) return true;
  if (vertex_accessor.out_degree() > 0 || vertex_accessor.in_degree() > 0)
    return false;

  vertex_accessor.vlist_->remove(vertex_accessor.current_, *transaction_);
  return true;
}

void GraphDbAccessor::DetachRemoveVertex(VertexAccessor &vertex_accessor) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  vertex_accessor.SwitchNew();
  for (auto edge_accessor : vertex_accessor.in())
    RemoveEdge(edge_accessor, true, false);
  vertex_accessor.SwitchNew();
  for (auto edge_accessor : vertex_accessor.out())
    RemoveEdge(edge_accessor, false, true);

  vertex_accessor.SwitchNew();
  // it's possible the vertex was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  if (!vertex_accessor.current_->is_expired_by(*transaction_))
    vertex_accessor.vlist_->remove(vertex_accessor.current_, *transaction_);
}

EdgeAccessor GraphDbAccessor::InsertEdge(VertexAccessor &from,
                                         VertexAccessor &to,
                                         GraphDbTypes::EdgeType edge_type) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  // create an edge
  auto edge_vlist = new mvcc::VersionList<Edge>(*transaction_, *from.vlist_,
                                                *to.vlist_, edge_type);
  // We need to insert edge_vlist to edges_ before calling update since update
  // can throw and edge_vlist will not be garbage collected if it is not in
  // edges_ skiplist.
  bool success = db_.edges_.access().insert(edge_vlist).second;
  CHECK(success) << "It is impossible for new version list to already exist";

  // ensure that the "from" accessor has the latest version
  from.SwitchNew();
  from.update().out_.emplace(to.vlist_, edge_vlist, edge_type);
  // ensure that the "to" accessor has the latest version
  // WARNING: must do that after the above "from.update()" for cases when
  // we are creating a cycle and "from" and "to" are the same vlist
  to.SwitchNew();
  to.update().in_.emplace(from.vlist_, edge_vlist, edge_type);

  // This has to be here because there is no additional method for setting edge
  // type.
  const auto edge_accessor = EdgeAccessor(*edge_vlist, *this);
  return edge_accessor;
}

int64_t GraphDbAccessor::EdgesCount() const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return db_.edges_.access().size();
}

void GraphDbAccessor::RemoveEdge(EdgeAccessor &edge_accessor,
                                 bool remove_from_from, bool remove_from_to) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  // it's possible the edge was removed already in this transaction
  // due to it getting matched multiple times by some patterns
  // we can only delete it once, so check if it's already deleted
  edge_accessor.SwitchNew();
  if (edge_accessor.current_->is_expired_by(*transaction_)) return;
  if (remove_from_from)
    edge_accessor.from().update().out_.RemoveEdge(edge_accessor.vlist_);
  if (remove_from_to)
    edge_accessor.to().update().in_.RemoveEdge(edge_accessor.vlist_);
  edge_accessor.vlist_->remove(edge_accessor.current_, *transaction_);
}

GraphDbTypes::Label GraphDbAccessor::Label(const std::string &label_name) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return &(*db_.labels_.access().insert(label_name).first);
}

const std::string &GraphDbAccessor::LabelName(
    const GraphDbTypes::Label label) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return *label;
}

GraphDbTypes::EdgeType GraphDbAccessor::EdgeType(
    const std::string &edge_type_name) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return &(*db_.edge_types_.access().insert(edge_type_name).first);
}

const std::string &GraphDbAccessor::EdgeTypeName(
    const GraphDbTypes::EdgeType edge_type) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return *edge_type;
}

GraphDbTypes::Property GraphDbAccessor::Property(
    const std::string &property_name) {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return &(*db_.properties_.access().insert(property_name).first);
}

const std::string &GraphDbAccessor::PropertyName(
    const GraphDbTypes::Property property) const {
  DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
  return *property;
}

int64_t GraphDbAccessor::Counter(const std::string &name) {
  return db_.counters_.access()
      .emplace(name, std::make_tuple(name), std::make_tuple(0))
      .first->second.fetch_add(1);
}

void GraphDbAccessor::CounterSet(const std::string &name, int64_t value) {
  auto name_counter_pair = db_.counters_.access().emplace(
      name, std::make_tuple(name), std::make_tuple(value));
  if (!name_counter_pair.second) name_counter_pair.first->second.store(value);
}

std::vector<std::string> GraphDbAccessor::IndexInfo() const {
  std::vector<std::string> info;
  for (GraphDbTypes::Label label : db_.labels_index_.Keys()) {
    info.emplace_back(":" + LabelName(label));
  }

  // Edge indices are not shown because they are never used.

  for (LabelPropertyIndex::Key key : db_.label_property_index_.Keys()) {
    info.emplace_back(fmt::format(":{}({})", LabelName(key.label_),
                                  PropertyName(key.property_)));
  }

  return info;
}
