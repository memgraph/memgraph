#pragma once

#include <optional>
#include <tuple>
#include <utility>

#include "storage/v2/property_value.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/bound.hpp"
#include "utils/skip_list.hpp"

namespace storage {

struct Indices;

class LabelIndex {
 private:
  struct Entry {
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs) {
      return std::make_tuple(vertex, timestamp) <
             std::make_tuple(rhs.vertex, rhs.timestamp);
    }
    bool operator==(const Entry &rhs) {
      return vertex == rhs.vertex && timestamp == rhs.timestamp;
    }
  };

  struct LabelStorage {
    LabelId label;
    utils::SkipList<Entry> vertices;

    bool operator<(const LabelStorage &rhs) { return label < rhs.label; }
    bool operator<(LabelId rhs) { return label < rhs; }
    bool operator==(const LabelStorage &rhs) { return label == rhs.label; }
    bool operator==(LabelId rhs) { return label == rhs; }
  };

 public:
  explicit LabelIndex(Indices *indices) : indices_(indices) {}

  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx);

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label,
             View view, Transaction *transaction, Indices *indices);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      VertexAccessor operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const {
        return index_iterator_ == other.index_iterator_;
      }
      bool operator!=(const Iterator &other) const {
        return index_iterator_ != other.index_iterator_;
      }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin() { return Iterator(this, index_accessor_.begin()); }
    Iterator end() { return Iterator(this, index_accessor_.end()); }

   private:
    utils::SkipList<Entry>::Accessor index_accessor_;
    LabelId label_;
    View view_;
    Transaction *transaction_;
    Indices *indices_;
  };

  /// Returns an self with vertices visible from the given transaction.
  Iterable Vertices(LabelId label, View view, Transaction *transaction) {
    return Iterable(GetOrCreateStorage(label)->access(), label, view,
                    transaction, indices_);
  }

 private:
  utils::SkipList<LabelStorage> index_;
  Indices *indices_;

  utils::SkipList<Entry> *GetOrCreateStorage(LabelId label);
};

class LabelPropertyIndex {
 private:
  struct Entry {
    PropertyValue value;
    Vertex *vertex;
    uint64_t timestamp;

    bool operator<(const Entry &rhs);
    bool operator==(const Entry &rhs);

    bool operator<(const PropertyValue &rhs);
    bool operator==(const PropertyValue &rhs);
  };

 public:
  explicit LabelPropertyIndex(Indices *indices) : indices_(indices) {}

  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx);

  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value,
                           Vertex *vertex, const Transaction &tx);

  bool CreateIndex(LabelId label, PropertyId property,
                   utils::SkipList<Vertex>::Accessor vertices);

  bool DropIndex(LabelId label, PropertyId property) {
    return index_.erase({label, property}) > 0;
  }

  bool IndexExists(LabelId label, PropertyId property) {
    return index_.find({label, property}) != index_.end();
  }

  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  class Iterable {
   public:
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label,
             PropertyId property,
             const std::optional<utils::Bound<PropertyValue>> &lower_bound,
             const std::optional<utils::Bound<PropertyValue>> &upper_bound,
             View view, Transaction *transaction, Indices *indices);

    class Iterator {
     public:
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      VertexAccessor operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const {
        return index_iterator_ == other.index_iterator_;
      }
      bool operator!=(const Iterator &other) const {
        return index_iterator_ != other.index_iterator_;
      }

      Iterator &operator++();

     private:
      void AdvanceUntilValid();

      Iterable *self_;
      utils::SkipList<Entry>::Iterator index_iterator_;
      VertexAccessor current_vertex_accessor_;
      Vertex *current_vertex_;
    };

    Iterator begin();
    Iterator end();

   private:
    utils::SkipList<Entry>::Accessor index_accessor_;
    LabelId label_;
    PropertyId property_;
    std::optional<utils::Bound<PropertyValue>> lower_bound_;
    std::optional<utils::Bound<PropertyValue>> upper_bound_;
    View view_;
    Transaction *transaction_;
    Indices *indices_;
  };

  Iterable Vertices(
      LabelId label, PropertyId property,
      const std::optional<utils::Bound<PropertyValue>> &lower_bound,
      const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
      Transaction *transaction) {
    auto it = index_.find({label, property});
    CHECK(it != index_.end())
        << "Index for label " << label.AsUint() << " and property "
        << property.AsUint() << " doesn't exist";
    return Iterable(it->second.access(), label, property, lower_bound,
                    upper_bound, view, transaction, indices_);
  }

 private:
  Indices *indices_;
  std::map<std::pair<LabelId, PropertyId>, utils::SkipList<Entry>> index_;
};

struct Indices {
  Indices() : label_index(this), label_property_index(this) {}

  // Disable copy and move because members hold pointer to `this`.
  Indices(const Indices &) = delete;
  Indices(Indices &&) = delete;
  Indices &operator=(const Indices &) = delete;
  Indices &operator=(Indices &&) = delete;
  ~Indices() = default;

  LabelIndex label_index;
  LabelPropertyIndex label_property_index;
};

/// This function should be called from garbage collection to clean-up the
/// index.
void RemoveObsoleteEntries(Indices *indices,
                           uint64_t oldest_active_start_timestamp);

// Indices are updated whenever an update occurs, instead of only on commit or
// advance command. This is necessary because we want indices to support `NEW`
// view for use in Merge.

/// This function should be called whenever a label is added to a vertex.
void UpdateOnAddLabel(Indices *indices, LabelId label, Vertex *vertex,
                      const Transaction &tx);

/// This function should be called whenever a property is modified on a vertex.
void UpdateOnSetProperty(Indices *indices, PropertyId property,
                         const PropertyValue &value, Vertex *vertex,
                         const Transaction &tx);

}  // namespace storage
