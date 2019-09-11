#include "indices.hpp"

#include "storage/v2/mvcc.hpp"

namespace storage {

namespace {

/// We treat doubles and integers the same for label property indices, so
/// special comparison operators are needed.
bool PropertyValueLess(const PropertyValue &lhs, const PropertyValue &rhs) {
  if (lhs.type() == rhs.type()) {
    return lhs < rhs;
  }
  if (lhs.IsInt() && rhs.IsDouble()) {
    return lhs.ValueInt() < rhs.ValueDouble();
  }
  if (lhs.IsDouble() && rhs.IsInt()) {
    return lhs.ValueDouble() < rhs.ValueInt();
  }
  return lhs.type() < rhs.type();
}

bool PropertyValueEqual(const PropertyValue &lhs, const PropertyValue &rhs) {
  if (lhs.type() == rhs.type()) {
    return lhs == rhs;
  }
  if (lhs.IsInt() && rhs.IsDouble()) {
    return lhs.ValueInt() == rhs.ValueDouble();
  }
  if (lhs.IsDouble() && rhs.IsInt()) {
    return lhs.ValueDouble() == rhs.ValueInt();
  }
  return false;
}

/// Traverses deltas visible from transaction with start timestamp greater than
/// the provided timestamp, and calls the provided callback function for each
/// delta. If the callback ever returns true, traversal is stopped and the
/// function returns true. Otherwise, the function returns false.
template <typename TCallback>
bool AnyVersionSatisfiesPredicate(uint64_t timestamp, Delta *delta,
                                  const TCallback &predicate) {
  while (delta != nullptr) {
    auto ts = delta->timestamp->load(std::memory_order_acquire);
    // This is a committed change that we see so we shouldn't undo it.
    if (ts < timestamp) {
      break;
    }
    if (predicate(*delta)) {
      return true;
    }
    // Move to the next delta.
    delta = delta->next.load(std::memory_order_acquire);
  }
  return false;
}

/// Helper function for label index garbage collection. Returns true if there's
/// a reachable version of the vertex that has the given label.
bool AnyVersionHasLabel(Vertex *vertex, LabelId label, uint64_t timestamp) {
  bool has_label;
  bool deleted;
  Delta *delta;
  {
    std::lock_guard<utils::SpinLock> guard(vertex->lock);
    has_label = utils::Contains(vertex->labels, label);
    deleted = vertex->deleted;
    delta = vertex->delta;
  }
  if (!deleted && has_label) {
    return true;
  }
  return AnyVersionSatisfiesPredicate(
      timestamp, delta, [&has_label, &deleted, label](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::ADD_LABEL:
            if (delta.label == label) {
              CHECK(!has_label) << "Invalid database state!";
              has_label = true;
            }
            break;
          case Delta::Action::REMOVE_LABEL:
            if (delta.label == label) {
              CHECK(has_label) << "Invalid database state!";
              has_label = false;
            }
            break;
          case Delta::Action::RECREATE_OBJECT: {
            CHECK(deleted) << "Invalid database state!";
            deleted = false;
            break;
          }
          case Delta::Action::DELETE_OBJECT: {
            CHECK(!deleted) << "Invalid database state!";
            deleted = true;
            break;
          }
          case Delta::Action::SET_PROPERTY:
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
        return !deleted && has_label;
      });
}

/// Helper function for label-property index garbage collection. Returns true if
/// there's a reachable version of the vertex that has the given label and
/// property value.
/// @throw std::bad_alloc if unable to copy the PropertyValue
bool AnyVersionHasLabelProperty(Vertex *vertex, LabelId label, PropertyId key,
                                const PropertyValue &value,
                                uint64_t timestamp) {
  bool has_label;
  PropertyValue current_value;
  bool deleted;
  Delta *delta;
  {
    std::lock_guard<utils::SpinLock> guard(vertex->lock);
    has_label = utils::Contains(vertex->labels, label);
    auto it = vertex->properties.find(key);
    if (it != vertex->properties.end()) {
      current_value = it->second;
    }
    deleted = vertex->deleted;
    delta = vertex->delta;
  }

  if (!deleted && has_label && PropertyValueEqual(current_value, value)) {
    return true;
  }

  return AnyVersionSatisfiesPredicate(
      timestamp, delta,
      [&has_label, &current_value, &deleted, label, key,
       value](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::ADD_LABEL:
            if (delta.label == label) {
              CHECK(!has_label) << "Invalid database state!";
              has_label = true;
            }
            break;
          case Delta::Action::REMOVE_LABEL:
            if (delta.label == label) {
              CHECK(has_label) << "Invalid database state!";
              has_label = false;
            }
            break;
          case Delta::Action::SET_PROPERTY:
            if (delta.property.key == key) {
              current_value = delta.property.value;
            }
            break;
          case Delta::Action::RECREATE_OBJECT: {
            CHECK(deleted) << "Invalid database state!";
            deleted = false;
            break;
          }
          case Delta::Action::DELETE_OBJECT: {
            CHECK(!deleted) << "Invalid database state!";
            deleted = true;
            break;
          }
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
        return !deleted && has_label &&
               PropertyValueEqual(current_value, value);
      });
}

// Helper function for iterating through label index. Returns true if this
// transaction can see the given vertex, and the visible version has the given
// label.
bool CurrentVersionHasLabel(Vertex *vertex, LabelId label,
                            Transaction *transaction, View view) {
  bool deleted;
  bool has_label;
  Delta *delta;
  {
    std::lock_guard<utils::SpinLock> guard(vertex->lock);
    deleted = vertex->deleted;
    has_label = utils::Contains(vertex->labels, label);
    delta = vertex->delta;
  }
  ApplyDeltasForRead(transaction, delta, view,
                     [&deleted, &has_label, label](const Delta &delta) {
                       switch (delta.action) {
                         case Delta::Action::REMOVE_LABEL: {
                           if (delta.label == label) {
                             CHECK(has_label) << "Invalid database state!";
                             has_label = false;
                           }
                           break;
                         }
                         case Delta::Action::ADD_LABEL: {
                           if (delta.label == label) {
                             CHECK(!has_label) << "Invalid database state!";
                             has_label = true;
                           }
                           break;
                         }
                         case Delta::Action::DELETE_OBJECT: {
                           CHECK(!deleted) << "Invalid database state!";
                           deleted = true;
                           break;
                         }
                         case Delta::Action::RECREATE_OBJECT: {
                           CHECK(deleted) << "Invalid database state!";
                           deleted = false;
                           break;
                         }
                         case Delta::Action::SET_PROPERTY:
                         case Delta::Action::ADD_IN_EDGE:
                         case Delta::Action::ADD_OUT_EDGE:
                         case Delta::Action::REMOVE_IN_EDGE:
                         case Delta::Action::REMOVE_OUT_EDGE:
                           break;
                       }
                     });
  return !deleted && has_label;
}

// Helper function for iterating through label-property index. Returns true if
// this transaction can see the given vertex, and the visible version has the
// given label and property.
// @throw std::bad_alloc if unable to copy the PropertyValue
bool CurrentVersionHasLabelProperty(Vertex *vertex, LabelId label,
                                    PropertyId key, const PropertyValue &value,
                                    Transaction *transaction, View view) {
  bool deleted;
  bool has_label;
  PropertyValue current_value;
  Delta *delta;
  {
    std::lock_guard<utils::SpinLock> guard(vertex->lock);
    deleted = vertex->deleted;
    has_label = utils::Contains(vertex->labels, label);
    auto it = vertex->properties.find(key);
    if (it != vertex->properties.end()) {
      current_value = it->second;
    }
    delta = vertex->delta;
  }
  ApplyDeltasForRead(
      transaction, delta, view,
      [&deleted, &has_label, &current_value, key, label](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::SET_PROPERTY: {
            if (delta.property.key == key) {
              current_value = delta.property.value;
            }
            break;
          }
          case Delta::Action::DELETE_OBJECT: {
            CHECK(!deleted) << "Invalid database state!";
            deleted = true;
            break;
          }
          case Delta::Action::RECREATE_OBJECT: {
            CHECK(deleted) << "Invalid database state!";
            deleted = false;
            break;
          }
          case Delta::Action::ADD_LABEL:
            if (delta.label == label) {
              CHECK(!has_label) << "Invalid database state!";
              has_label = true;
            }
            break;
          case Delta::Action::REMOVE_LABEL:
            if (delta.label == label) {
              CHECK(has_label) << "Invalid database state!";
              has_label = false;
            }
            break;
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
        }
      });
  return !deleted && has_label && PropertyValueEqual(current_value, value);
}

}  // namespace

void LabelIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex,
                                  const Transaction &tx) {
  GetOrCreateStorage(label)->access().insert(Entry{vertex, tx.start_timestamp});
}

std::vector<LabelId> LabelIndex::ListIndices() const {
  std::vector<LabelId> ret;
  ret.reserve(index_.size());
  auto acc = index_.access();
  for (const auto &item : acc) {
    ret.push_back(item.label);
  }
  return ret;
}

void LabelIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  auto index_acc = index_.access();
  for (auto &label_storage : index_acc) {
    auto vertices_acc = label_storage.vertices.access();
    for (auto it = vertices_acc.begin(); it != vertices_acc.end();) {
      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      if ((next_it != vertices_acc.end() && it->vertex == next_it->vertex) ||
          !AnyVersionHasLabel(it->vertex, label_storage.label,
                              oldest_active_start_timestamp)) {
        vertices_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

utils::SkipList<LabelIndex::Entry> *LabelIndex::GetOrCreateStorage(
    LabelId label) {
  auto acc = index_.access();
  auto it = acc.find(label);
  if (it == acc.end()) {
    LabelStorage label_storage{.label = label,
                               .vertices = utils::SkipList<Entry>()};
    it = acc.insert(std::move(label_storage)).first;
  }
  return &it->vertices;
}

LabelIndex::Iterable::Iterator::Iterator(
    Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, nullptr, nullptr),
      current_vertex_(nullptr) {
  AdvanceUntilValid();
}

LabelIndex::Iterable::Iterator &LabelIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void LabelIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    if (index_iterator_->vertex == current_vertex_) {
      continue;
    }
    if (CurrentVersionHasLabel(index_iterator_->vertex, self_->label_,
                               self_->transaction_, self_->view_)) {
      current_vertex_ = index_iterator_->vertex;
      current_vertex_accessor_ =
          VertexAccessor{current_vertex_, self_->transaction_, self_->indices_};
      break;
    }
  }
}

LabelIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor,
                               LabelId label, View view,
                               Transaction *transaction, Indices *indices)
    : index_accessor_(std::move(index_accessor)),
      label_(label),
      view_(view),
      transaction_(transaction),
      indices_(indices) {}

bool LabelPropertyIndex::Entry::operator<(const Entry &rhs) {
  if (PropertyValueLess(value, rhs.value)) {
    return true;
  }
  if (PropertyValueLess(rhs.value, value)) {
    return false;
  }
  return std::make_tuple(vertex, timestamp) <
         std::make_tuple(rhs.vertex, rhs.timestamp);
}

bool LabelPropertyIndex::Entry::operator==(const Entry &rhs) {
  return PropertyValueEqual(value, rhs.value) && vertex == rhs.vertex &&
         timestamp == rhs.timestamp;
}

bool LabelPropertyIndex::Entry::operator<(const PropertyValue &rhs) {
  return PropertyValueLess(value, rhs);
}

bool LabelPropertyIndex::Entry::operator==(const PropertyValue &rhs) {
  return PropertyValueEqual(value, rhs);
}

void LabelPropertyIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex,
                                          const Transaction &tx) {
  for (auto &[label_prop, storage] : index_) {
    if (label_prop.first != label) {
      continue;
    }
    PropertyValue value;
    {
      utils::SpinLock guard(vertex->lock);
      auto it = vertex->properties.find(label_prop.second);
      if (it != vertex->properties.end()) {
        value = it->second;
      }
    }
    if (!value.IsNull()) {
      storage.access().insert(Entry{value, vertex, tx.start_timestamp});
    }
  }
}

void LabelPropertyIndex::UpdateOnSetProperty(PropertyId property,
                                             const PropertyValue &value,
                                             Vertex *vertex,
                                             const Transaction &tx) {
  if (value.IsNull()) {
    return;
  }
  for (auto &[label_prop, storage] : index_) {
    if (label_prop.second != property) {
      continue;
    }
    bool has_label;
    {
      utils::SpinLock guard(vertex->lock);
      has_label = utils::Contains(vertex->labels, label_prop.first);
    }
    if (has_label) {
      storage.access().insert(Entry{value, vertex, tx.start_timestamp});
    }
  }
}

bool LabelPropertyIndex::CreateIndex(
    LabelId label, PropertyId property,
    utils::SkipList<Vertex>::Accessor vertices) {
  auto [it, emplaced] = index_.emplace(std::piecewise_construct,
                                       std::forward_as_tuple(label, property),
                                       std::forward_as_tuple());
  if (!emplaced) {
    // Index already exists.
    return false;
  }
  auto acc = it->second.access();
  for (Vertex &vertex : vertices) {
    if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
      continue;
    }
    auto it = vertex.properties.find(property);
    if (it == vertex.properties.end()) {
      continue;
    }
    acc.insert(Entry{it->second, &vertex, 0});
  }
  return true;
}

std::vector<std::pair<LabelId, PropertyId>> LabelPropertyIndex::ListIndices()
    const {
  std::vector<std::pair<LabelId, PropertyId>> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back(item.first);
  }
  return ret;
}

void LabelPropertyIndex::RemoveObsoleteEntries(
    uint64_t oldest_active_start_timestamp) {
  for (auto &[label_property, index] : index_) {
    auto index_acc = index.access();
    for (auto it = index_acc.begin(); it != index_acc.end();) {
      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      if ((next_it != index_acc.end() && it->vertex == next_it->vertex &&
           PropertyValueEqual(it->value, next_it->value)) ||
          !AnyVersionHasLabelProperty(it->vertex, label_property.first,
                                      label_property.second, it->value,
                                      oldest_active_start_timestamp)) {
        index_acc.remove(*it);
      }
      it = next_it;
    }
  }
}

LabelPropertyIndex::Iterable::Iterator::Iterator(
    Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, nullptr, nullptr),
      current_vertex_(nullptr) {
  AdvanceUntilValid();
}

LabelPropertyIndex::Iterable::Iterator &LabelPropertyIndex::Iterable::Iterator::
operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void LabelPropertyIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    if (index_iterator_->vertex == current_vertex_) {
      continue;
    }

    if (self_->lower_bound_) {
      if (PropertyValueLess(index_iterator_->value,
                            self_->lower_bound_->value())) {
        continue;
      }
      if (!self_->lower_bound_->IsInclusive() &&
          PropertyValueEqual(index_iterator_->value,
                             self_->lower_bound_->value())) {
        continue;
      }
    }
    if (self_->upper_bound_) {
      if (PropertyValueLess(self_->upper_bound_->value(),
                            index_iterator_->value)) {
        index_iterator_ = self_->index_accessor_.end();
        break;
      }
      if (!self_->upper_bound_->IsInclusive() &&
          PropertyValueEqual(index_iterator_->value,
                             self_->upper_bound_->value())) {
        index_iterator_ = self_->index_accessor_.end();
        break;
      }
    }

    if (CurrentVersionHasLabelProperty(index_iterator_->vertex, self_->label_,
                                       self_->property_, index_iterator_->value,
                                       self_->transaction_, self_->view_)) {
      current_vertex_ = index_iterator_->vertex;
      current_vertex_accessor_ =
          VertexAccessor(current_vertex_, self_->transaction_, self_->indices_);
      break;
    }
  }
}

LabelPropertyIndex::Iterable::Iterable(
    utils::SkipList<Entry>::Accessor index_accessor, LabelId label,
    PropertyId property,
    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
    Transaction *transaction, Indices *indices)
    : index_accessor_(std::move(index_accessor)),
      label_(label),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      view_(view),
      transaction_(transaction),
      indices_(indices) {}

LabelPropertyIndex::Iterable::Iterator LabelPropertyIndex::Iterable::begin() {
  auto index_iterator = index_accessor_.begin();
  if (lower_bound_) {
    index_iterator =
        index_accessor_.find_equal_or_greater(lower_bound_->value());
  }
  return Iterator(this, index_iterator);
}

LabelPropertyIndex::Iterable::Iterator LabelPropertyIndex::Iterable::end() {
  return Iterator(this, index_accessor_.end());
}

// A helper function for determining the skip list layer used for estimating the
// number of elements in the label property index. The lower layer we use, the
// better approximation we get (if we use the lowest layer, we get the exact
// numbers). However, lower skip list layers contain more elements so we must
// iterate through more items to get the estimate.
//
// Our goal is to achieve balance between execution time and approximation
// precision. The expected number of elements at the k-th skip list layer is N *
// (1/2)^(k-1), where N is the skip-list size. We choose to iterate through no
// more than sqrt(N) items for large N when calculating the estimate, so we need
// to choose the skip-list layer such that N * (1/2)^(k-1) <= sqrt(N). That is
// equivalent to k >= 1 + 1/2 * log2(N), so we choose k to be 1 + ceil(log2(N) /
// 2).
//
// For N small enough (arbitrarily chosen to be 500), we will just use the
// lowest layer to get the exact numbers. Mostly because this makes writing
// tests easier.
namespace {
uint64_t SkipListLayerForEstimation(uint64_t N) {
  if (N <= 500) return 1;
  return std::min(1 + (utils::Log2(N) + 1) / 2, utils::kSkipListMaxHeight);
}
}  // namespace

int64_t LabelPropertyIndex::ApproximateVertexCount(
    LabelId label, PropertyId property, const PropertyValue &value) const {
  auto it = index_.find({label, property});
  CHECK(it != index_.end())
      << "Index for label " << label.AsUint() << " and property "
      << property.AsUint() << " doesn't exist";
  auto acc = it->second.access();
  return acc.estimate_count(value, SkipListLayerForEstimation(acc.size()));
}

int64_t LabelPropertyIndex::ApproximateVertexCount(
    LabelId label, PropertyId property,
    const std::optional<utils::Bound<PropertyValue>> &lower,
    const std::optional<utils::Bound<PropertyValue>> &upper) const {
  auto it = index_.find({label, property});
  CHECK(it != index_.end())
      << "Index for label " << label.AsUint() << " and property "
      << property.AsUint() << " doesn't exist";
  auto acc = it->second.access();
  return acc.estimate_range_count(lower, upper,
                                  SkipListLayerForEstimation(acc.size()));
}

void RemoveObsoleteEntries(Indices *indices,
                           uint64_t oldest_active_start_timestamp) {
  indices->label_index.RemoveObsoleteEntries(oldest_active_start_timestamp);
  indices->label_property_index.RemoveObsoleteEntries(
      oldest_active_start_timestamp);
}

void UpdateOnAddLabel(Indices *indices, LabelId label, Vertex *vertex,
                      const Transaction &tx) {
  indices->label_index.UpdateOnAddLabel(label, vertex, tx);
  indices->label_property_index.UpdateOnAddLabel(label, vertex, tx);
}

void UpdateOnSetProperty(Indices *indices, PropertyId property,
                         const PropertyValue &value, Vertex *vertex,
                         const Transaction &tx) {
  indices->label_property_index.UpdateOnSetProperty(property, value, vertex,
                                                    tx);
}

}  // namespace storage
