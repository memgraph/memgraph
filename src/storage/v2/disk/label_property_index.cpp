// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/inmemory/indices_utils.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

namespace {
constexpr const char *label_property_index_path = "rocksdb_label_property_index";
}  // namespace

DiskLabelPropertyIndex::DiskLabelPropertyIndex(Indices *indices, Constraints *constraints, Config::Items config)
    : LabelPropertyIndex(indices, constraints, config) {
  std::filesystem::path rocksdb_path = label_property_index_path;
  kvstore_ = std::make_unique<RocksDBStorage>();
  utils::EnsureDirOrDie(rocksdb_path);
  kvstore_->options_.create_if_missing = true;
  //   // kvstore_->options_.comparator = new ComparatorWithU64TsImpl();
  logging::AssertRocksDBStatus(rocksdb::DB::Open(kvstore_->options_, rocksdb_path, &kvstore_->db_));
}

bool DiskLabelPropertyIndex::Entry::operator<(const Entry &rhs) {
  if (value < rhs.value) {
    return true;
  }
  if (rhs.value < value) {
    return false;
  }
  return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
}

bool DiskLabelPropertyIndex::Entry::operator==(const Entry &rhs) {
  return value == rhs.value && vertex == rhs.vertex && timestamp == rhs.timestamp;
}

bool DiskLabelPropertyIndex::Entry::operator<(const PropertyValue &rhs) { return value < rhs; }

bool DiskLabelPropertyIndex::Entry::operator==(const PropertyValue &rhs) { return value == rhs; }

void DiskLabelPropertyIndex::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) {
  /// TODO: andi iterate over whole set
  // throw utils::NotYetImplemented("DiskLabelPropertyIndex::UpdateOnAddLabel");
}

void DiskLabelPropertyIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                                 const Transaction &tx) {
  if (value.IsNull()) {
    return;
  }
  /// TODO: andi iterate over whole set
  // throw utils::NotYetImplemented("DiskLabelPropertyIndex::UpdateOnSetProperty");
}

bool DiskLabelPropertyIndex::CreateIndex(LabelId label, PropertyId property,
                                         const std::vector<std::pair<std::string, std::string>> &vertices) {
  index_.emplace(label, property);
  rocksdb::WriteOptions wo;
  for (const auto &[key, value] : vertices) {
    kvstore_->db_->Put(wo, key, value);
  }
  return true;
}

bool DiskLabelPropertyIndex::DropIndex(LabelId label, PropertyId property) {
  return index_.erase({label, property}) > 0;
}

bool DiskLabelPropertyIndex::IndexExists(LabelId label, PropertyId property) const {
  return index_.find({label, property}) != index_.end();
}

std::vector<std::pair<LabelId, PropertyId>> DiskLabelPropertyIndex::ListIndices() const {
  return std::vector<std::pair<LabelId, PropertyId>>(index_.begin(), index_.end());
}

void DiskLabelPropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp) {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::RemoveObsoleteEntries");
}

DiskLabelPropertyIndex::Iterable::Iterator::Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, nullptr, nullptr, nullptr, self_->config_),
      current_vertex_(nullptr) {}

DiskLabelPropertyIndex::Iterable::Iterator &DiskLabelPropertyIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  return *this;
}

// These constants represent the smallest possible value of each type that is
// contained in a `PropertyValue`. Note that numbers (integers and doubles) are
// treated as the same "type" in `PropertyValue`.
const PropertyValue kSmallestBool = PropertyValue(false);
static_assert(-std::numeric_limits<double>::infinity() < std::numeric_limits<int64_t>::min());
const PropertyValue kSmallestNumber = PropertyValue(-std::numeric_limits<double>::infinity());
const PropertyValue kSmallestString = PropertyValue("");
const PropertyValue kSmallestList = PropertyValue(std::vector<PropertyValue>());
const PropertyValue kSmallestMap = PropertyValue(std::map<std::string, PropertyValue>());
const PropertyValue kSmallestTemporalData =
    PropertyValue(TemporalData{static_cast<TemporalType>(0), std::numeric_limits<int64_t>::min()});

DiskLabelPropertyIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label,
                                           PropertyId property,
                                           const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                           const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                           Transaction *transaction, Indices *indices, Constraints *constraints,
                                           Config::Items config)
    : index_accessor_(std::move(index_accessor)),
      label_(label),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      view_(view),
      transaction_(transaction),
      indices_(indices),
      constraints_(constraints),
      config_(config) {
  // We have to fix the bounds that the user provided to us. If the user
  // provided only one bound we should make sure that only values of that type
  // are returned by the iterator. We ensure this by supplying either an
  // inclusive lower bound of the same type, or an exclusive upper bound of the
  // following type. If neither bound is set we yield all items in the index.

  // First we statically verify that our assumptions about the `PropertyValue`
  // type ordering holds.
  static_assert(PropertyValue::Type::Bool < PropertyValue::Type::Int);
  static_assert(PropertyValue::Type::Int < PropertyValue::Type::Double);
  static_assert(PropertyValue::Type::Double < PropertyValue::Type::String);
  static_assert(PropertyValue::Type::String < PropertyValue::Type::List);
  static_assert(PropertyValue::Type::List < PropertyValue::Type::Map);

  // Remove any bounds that are set to `Null` because that isn't a valid value.
  if (lower_bound_ && lower_bound_->value().IsNull()) {
    lower_bound_ = std::nullopt;
  }
  if (upper_bound_ && upper_bound_->value().IsNull()) {
    upper_bound_ = std::nullopt;
  }

  // Check whether the bounds are of comparable types if both are supplied.
  if (lower_bound_ && upper_bound_ &&
      !PropertyValue::AreComparableTypes(lower_bound_->value().type(), upper_bound_->value().type())) {
    bounds_valid_ = false;
    return;
  }

  // Set missing bounds.
  if (lower_bound_ && !upper_bound_) {
    // Here we need to supply an upper bound. The upper bound is set to an
    // exclusive lower bound of the following type.
    switch (lower_bound_->value().type()) {
      case PropertyValue::Type::Null:
        // This shouldn't happen because of the nullopt-ing above.
        LOG_FATAL("Invalid database state!");
        break;
      case PropertyValue::Type::Bool:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestNumber);
        break;
      case PropertyValue::Type::Int:
      case PropertyValue::Type::Double:
        // Both integers and doubles are treated as the same type in
        // `PropertyValue` and they are interleaved when sorted.
        upper_bound_ = utils::MakeBoundExclusive(kSmallestString);
        break;
      case PropertyValue::Type::String:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestList);
        break;
      case PropertyValue::Type::List:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestMap);
        break;
      case PropertyValue::Type::Map:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestTemporalData);
        break;
      case PropertyValue::Type::TemporalData:
        // This is the last type in the order so we leave the upper bound empty.
        break;
    }
  }
  if (upper_bound_ && !lower_bound_) {
    // Here we need to supply a lower bound. The lower bound is set to an
    // inclusive lower bound of the current type.
    switch (upper_bound_->value().type()) {
      case PropertyValue::Type::Null:
        // This shouldn't happen because of the nullopt-ing above.
        LOG_FATAL("Invalid database state!");
        break;
      case PropertyValue::Type::Bool:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestBool);
        break;
      case PropertyValue::Type::Int:
      case PropertyValue::Type::Double:
        // Both integers and doubles are treated as the same type in
        // `PropertyValue` and they are interleaved when sorted.
        lower_bound_ = utils::MakeBoundInclusive(kSmallestNumber);
        break;
      case PropertyValue::Type::String:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestString);
        break;
      case PropertyValue::Type::List:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestList);
        break;
      case PropertyValue::Type::Map:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestMap);
        break;
      case PropertyValue::Type::TemporalData:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestTemporalData);
        break;
    }
  }
}

DiskLabelPropertyIndex::Iterable::Iterator DiskLabelPropertyIndex::Iterable::begin() {
  // If the bounds are set and don't have comparable types we don't yield any
  // items from the index.
  if (!bounds_valid_) return Iterator(this, index_accessor_.end());
  auto index_iterator = index_accessor_.begin();
  if (lower_bound_) {
    index_iterator = index_accessor_.find_equal_or_greater(lower_bound_->value());
  }
  return Iterator(this, index_iterator);
}

DiskLabelPropertyIndex::Iterable::Iterator DiskLabelPropertyIndex::Iterable::end() {
  return Iterator(this, index_accessor_.end());
}

int64_t DiskLabelPropertyIndex::ApproximateVertexCount(LabelId label, PropertyId property) const {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::ApproximateVertexCount");
}

int64_t DiskLabelPropertyIndex::ApproximateVertexCount(LabelId label, PropertyId property,
                                                       const PropertyValue &value) const {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::ApproximateVertexCount");
}

int64_t DiskLabelPropertyIndex::ApproximateVertexCount(LabelId label, PropertyId property,
                                                       const std::optional<utils::Bound<PropertyValue>> &lower,
                                                       const std::optional<utils::Bound<PropertyValue>> &upper) const {
  throw utils::NotYetImplemented("DiskLabelPropertyIndex::ApproximateVertexCount");
}

std::vector<std::pair<LabelId, PropertyId>> DiskLabelPropertyIndex::DeleteIndexStatsForLabel(
    const storage::LabelId &label) {
  std::vector<std::pair<LabelId, PropertyId>> deleted_indexes;
  for (auto it = stats_.cbegin(); it != stats_.cend();) {
    if (it->first.first == label) {
      deleted_indexes.push_back(it->first);
      it = stats_.erase(it);
    } else {
      ++it;
    }
  }
  return deleted_indexes;
}

std::vector<std::pair<LabelId, PropertyId>> DiskLabelPropertyIndex::ClearIndexStats() {
  std::vector<std::pair<LabelId, PropertyId>> deleted_indexes;
  deleted_indexes.reserve(stats_.size());
  std::transform(stats_.begin(), stats_.end(), std::back_inserter(deleted_indexes),
                 [](const auto &elem) { return elem.first; });
  stats_.clear();
  return deleted_indexes;
}

void DiskLabelPropertyIndex::SetIndexStats(const storage::LabelId &label, const storage::PropertyId &property,
                                           const IndexStats &stats) {
  stats_[{label, property}] = stats;
}

std::optional<IndexStats> DiskLabelPropertyIndex::GetIndexStats(const storage::LabelId &label,
                                                                const storage::PropertyId &property) const {
  if (auto it = stats_.find({label, property}); it != stats_.end()) {
    return it->second;
  }
  return {};
}

void DiskLabelPropertyIndex::Clear() { index_.clear(); }

void DiskLabelPropertyIndex::RunGC() { throw utils::NotYetImplemented("DiskLabelPropertyIndex::RunGC"); }

}  // namespace memgraph::storage
