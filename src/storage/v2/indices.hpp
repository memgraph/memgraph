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

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, utils::SkipList<Vertex>::Accessor vertices);

  bool DropIndex(LabelId label) { return index_.erase(label) > 0; }

  bool IndexExists(LabelId label) const {
    return index_.find(label) != index_.end();
  }

  std::vector<LabelId> ListIndices() const;

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
    auto it = index_.find(label);
    CHECK(it != index_.end())
        << "Index for label " << label.AsUint() << " doesn't exist";
    return Iterable(it->second.access(), label, view, transaction, indices_);
  }

  int64_t ApproximateVertexCount(LabelId label) {
    auto it = index_.find(label);
    CHECK(it != index_.end())
        << "Index for label " << label.AsUint() << " doesn't exist";
    return it->second.size();
  }

 private:
  Indices *indices_;
  std::map<LabelId, utils::SkipList<Entry>> index_;
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

  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx);

  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value,
                           Vertex *vertex, const Transaction &tx);

  /// @throw std::bad_alloc
  bool CreateIndex(LabelId label, PropertyId property,
                   utils::SkipList<Vertex>::Accessor vertices);

  bool DropIndex(LabelId label, PropertyId property) {
    return index_.erase({label, property}) > 0;
  }

  bool IndexExists(LabelId label, PropertyId property) const {
    return index_.find({label, property}) != index_.end();
  }

  std::vector<std::pair<LabelId, PropertyId>> ListIndices() const;

  /// @throw std::bad_alloc if unable to copy a PropertyValue
  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp);

  class Iterable {
   public:
    /// @throw std::bad_alloc if unable to copy a PropertyValue
    Iterable(utils::SkipList<Entry>::Accessor index_accessor, LabelId label,
             PropertyId property,
             const std::optional<utils::Bound<PropertyValue>> &lower_bound,
             const std::optional<utils::Bound<PropertyValue>> &upper_bound,
             View view, Transaction *transaction, Indices *indices);

    class Iterator {
     public:
      /// @throw std::bad_alloc raised in AdvanceUntilValid
      Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator);

      VertexAccessor operator*() const { return current_vertex_accessor_; }

      bool operator==(const Iterator &other) const {
        return index_iterator_ == other.index_iterator_;
      }
      bool operator!=(const Iterator &other) const {
        return index_iterator_ != other.index_iterator_;
      }

      /// @throw std::bad_alloc raised in AdvanceUntilValid
      Iterator &operator++();

     private:
      /// @throw std::bad_alloc if unable to copy a PropertyValue
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

  /// @throw std::bad_alloc if unable to copy a PropertyValue
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

  int64_t ApproximateVertexCount(LabelId label, PropertyId property) const {
    auto it = index_.find({label, property});
    CHECK(it != index_.end())
        << "Index for label " << label.AsUint() << " and property "
        << property.AsUint() << " doesn't exist";
    return it->second.size();
  }

  int64_t ApproximateVertexCount(LabelId label, PropertyId property,
                                 const PropertyValue &value) const;

  int64_t ApproximateVertexCount(
      LabelId label, PropertyId property,
      const std::optional<utils::Bound<PropertyValue>> &lower,
      const std::optional<utils::Bound<PropertyValue>> &upper) const;

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
/// @throw std::bad_alloc raised in LabelPropertyIndex::RemoveObsoleteEntries
void RemoveObsoleteEntries(Indices *indices,
                           uint64_t oldest_active_start_timestamp);

// Indices are updated whenever an update occurs, instead of only on commit or
// advance command. This is necessary because we want indices to support `NEW`
// view for use in Merge.

/// This function should be called whenever a label is added to a vertex.
/// @throw std::bad_alloc
void UpdateOnAddLabel(Indices *indices, LabelId label, Vertex *vertex,
                      const Transaction &tx);

/// This function should be called whenever a property is modified on a vertex.
/// @throw std::bad_alloc
void UpdateOnSetProperty(Indices *indices, PropertyId property,
                         const PropertyValue &value, Vertex *vertex,
                         const Transaction &tx);

}  // namespace storage
