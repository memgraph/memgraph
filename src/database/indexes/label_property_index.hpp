#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_datatypes.hpp"
#include "database/indexes/index_common.hpp"
#include "mvcc/version_list.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "transactions/transaction.hpp"
#include "utils/total_ordering.hpp"

/**
 * @brief Implements LabelPropertyIndex.
 * Currently this provides implementation for:
 *    acquiring all entries which contain the given label, and a given property
 * sorted by the property value
 *    acquiring all non-unique entries with the given label, and property, with
 * exactly one property value
 */
class LabelPropertyIndex {
 public:
  LabelPropertyIndex(){};
  LabelPropertyIndex(const LabelPropertyIndex &other) = delete;
  LabelPropertyIndex(LabelPropertyIndex &&other) = delete;
  LabelPropertyIndex &operator=(const LabelPropertyIndex &other) = delete;
  LabelPropertyIndex &operator=(LabelPropertyIndex &&other) = delete;

  /**
   * @brief - Clear all indices so that we don't leak memory.
   */
  ~LabelPropertyIndex() {
    for (auto key_indices_pair : indices_.access()) {
      // Delete skiplist because we created it with a new operator.
      delete key_indices_pair.second;
    }
  }

  /**
   * @brief - Contain Label + property, to be used as an index key.
   */
  class Key : public TotalOrdering<Key> {
   public:
    const GraphDbTypes::Label label_;
    const GraphDbTypes::Property property_;

    Key(const GraphDbTypes::Label &label,
        const GraphDbTypes::Property &property)
        : label_(label), property_(property) {}

    // Comparison operators - we need them to keep this sorted inside skiplist.
    bool operator<(const Key &other) const {
      if (this->label_ != other.label_) return this->label_ < other.label_;
      return this->property_ < other.property_;
    }

    bool operator==(const Key &other) const {
      return this->label_ == other.label_ && this->property_ == other.property_;
    }
  };

  /**
   * @brief - Creates index with the given key if it doesn't exist. Note that
   * you still need to populate the index with existing records.
   * @return - True if it created the index, false if it already exists.
   */
  bool CreateIndex(const Key &key) {
    auto access = indices_.access();
    // Avoid creation if it already exists.
    auto iter = access.find(key);
    if (iter != access.end()) return false;

    auto skiplist = new SkipList<IndexEntry>;
    auto ret = access.insert(key, skiplist);
    // Avoid multithreaded memory leak if we don't delete skiplist and fail the
    // insert (some other thread already inserted)
    if (ret.second == false) delete skiplist;
    return ret.second;
  }

  /**
   * @brief - Notify that the index has been populated with everything it should
   * be populated with, and can be used from this moment forward without missing
   * any records.
   * @param key - index which finished being populated.
  */
  void IndexFinishedBuilding(const Key &key) {
    ready_for_use_.access().insert(key);
  }

  /**
   * @brief - Updates all indexes which should contain this vertex.
   * @param vlist - pointer to vlist entry to add
   * @param vertex - pointer to vertex record entry to add (contained in vlist)
   */
  void UpdateOnLabelProperty(mvcc::VersionList<Vertex> *const vlist,
                             const Vertex *const vertex) {
    const auto &labels = vertex->labels_;
    for (auto index : indices_.access()) {
      // Vertex has the given label
      if (std::find(labels.begin(), labels.end(), index.first.label_) ==
          labels.end())
        continue;
      auto prop = vertex->properties_.at(index.first.property_);
      if (prop.type() != PropertyValue::Type::Null) {
        // Property exists and vertex should be added to skiplist.
        Insert(*index.second, prop, vlist, vertex);
      }
    }
  }

  /**
   * @brief - Updates all indexes with `label` and any property in `vertex` that
   * exists.
   * @param label - indexes with this label might be updated if vertex contains
   * the corresponding property.
   * @param vlist - pointer to vlist entry to add
   * @param vertex - pointer to vertex record entry to add (contained in vlist)
   */
  void UpdateOnLabel(const GraphDbTypes::Label &label,
                     mvcc::VersionList<Vertex> *const vlist,
                     const Vertex *const vertex) {
    for (auto index : indices_.access()) {
      if (index.first.label_ != label) continue;
      auto prop = vertex->properties_.at(index.first.property_);
      if (prop.type() != PropertyValue::Type::Null) {
        // Property exists and vertex should be added to skiplist.
        Insert(*index.second, prop, vlist, vertex);
      }
    }
  }

  /**
   * @brief - Updates all indexes with `property` and any label in `vertex` that
   * exists.
   * @param property - indexes with this property might be updated if vertex
   * contains the corresponding label.
   * @param vlist - pointer to vlist entry to add
   * @param vertex - pointer to vertex record entry to add (contained in vlist)
   */
  void UpdateOnProperty(const GraphDbTypes::Property &property,
                        mvcc::VersionList<Vertex> *const vlist,
                        const Vertex *const vertex) {
    const auto &labels = vertex->labels_;
    for (auto index : indices_.access()) {
      if (index.first.property_ != property) continue;
      if (std::find(labels.begin(), labels.end(), index.first.label_) !=
          labels.end()) {
        // Label exists and vertex should be added to skiplist.
        Insert(*index.second, vertex->properties_.at(property), vlist, vertex);
      }
    }
  }

  /**
   * @brief - Get all the inserted vlists in key specific storage which still
   * have that label and property visible in this transaction.
   * @param key - Label+Property to query.
   * @param t - current transaction, which determines visibility.
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   * @return iterable collection of vlists of vertex records with the requested
   * key sorted ascendingly by the property value.
   */
  auto GetVlists(const Key &key, const tx::Transaction &t, bool current_state) {
    debug_assert(ready_for_use_.access().contains(key), "Index not yet ready.");
    auto access = GetKeyStorage(key)->access();
    auto begin = access.begin();
    return IndexUtils::GetVlists<typename SkipList<IndexEntry>::Iterator,
                                 IndexEntry, Vertex>(
        std::move(access), begin, [](const IndexEntry &) { return true; }, t,
        [key](const IndexEntry &entry, const Vertex *const vertex) {
          return LabelPropertyIndex::Exists(key, entry.value_, vertex);
        },
        current_state);
  }

  /**
   * @brief - Get all the inserted vlists in key specific storage which still
   * have that label and property visible in this transaction with property
   * value equal to 'value'.
   * @param key - Label+Property to query.
   * @param value - vlists with this value will be returned
   * @param t - current transaction, which determines visibility.
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   * @return iterable collection of vlists of vertex records with the requested
   * key and value
   */
  auto GetVlists(const Key &key, const PropertyValue &value,
                 const tx::Transaction &t, bool current_state) {
    debug_assert(ready_for_use_.access().contains(key), "Index not yet ready.");
    auto access = GetKeyStorage(key)->access();
    auto start_iter =
        access.find_or_larger<typename SkipList<IndexEntry>::Iterator,
                              IndexEntry>(IndexEntry(value, nullptr, nullptr));
    return IndexUtils::GetVlists<typename SkipList<IndexEntry>::Iterator,
                                 IndexEntry, Vertex>(
        std::move(access), start_iter,
        [value](const IndexEntry &entry) {
          return !IndexEntry::Less(value, entry.value_) &&
                 !IndexEntry::Less(entry.value_, value);
        },
        t,
        [key](const IndexEntry &entry, const Vertex *const vertex) {
          return LabelPropertyIndex::Exists(key, entry.value_, vertex);
        },
        current_state);
  }

  /**
   * @brief - Check for existance of index.
   * @param key - Index key
   * @return true if the index with that key exists
   */
  bool IndexExists(const Key &key) {
    return ready_for_use_.access().contains(key);
  }

  /**
   * @brief - Return number of items in skiplist associated with the given
   * key. This number could be imprecise because of the underlying skiplist
   * storage. Use this as a hint, and not as a rule. Fails if index doesn't
   * exist.
   * Moreover, some transaction probably sees only part of the skiplist since
   * not all versions are visible for it. Also, garbage collection might now
   * have been run for some time so the index might have accumulated garbage.
   * @param key - key to query for.
   * @return number of items
   */
  int64_t Count(const Key &key) {
    auto index = GetKeyStorage(key);
    permanent_assert(index != nullptr, "Index doesn't exist.");
    debug_assert(ready_for_use_.access().contains(key), "Index not yet ready.");
    return index->access().size();
  }

  /**
   * Returns the approximate position and count of the given value in the
   * index for the given Key.
   *
   * Both are approximations for several reasons. Initially the position
   * and count are obtained from the skipist (the index) and as such are
   * not exact for perfromance reasons. At the same time the position
   * and count are calculated based on property value comparison: an
   * additional error is accumulated because the index could contain
   * the same vertex with the same value multiple times,
   * as well as the same vertex with different values.
   */
  auto PositionAndCount(const Key &key, const PropertyValue &value) {
    auto access = GetKeyStorage(key)->access();
    return access.position_and_count(
        value,
        // the 'less' function
        [](const PropertyValue &a, const IndexEntry &b) {
          return IndexEntry::Less(a, b.value_);
        },
        // the 'equal_to' function
        [](const PropertyValue &a, const IndexEntry &b) {
          return !(IndexEntry::Less(a, b.value_) ||
                   IndexEntry::Less(b.value_, a));
        });
  }

  /**
   * @brief - Removes from the index all entries for which records don't contain
   * the given label anymore, or the record was deleted before this transaction
   * id.
   *
   * @param snapshot - the GC snapshot. Consists of the oldest active
   * transaction's snapshot, with that transaction's id appened as last.
   */
  void Refresh(const tx::Snapshot &snapshot, tx::Engine &engine) {
    return IndexUtils::Refresh<Key, IndexEntry, Vertex>(
        indices_, snapshot, engine,
        [](const Key &key, const IndexEntry &entry) {
          return LabelPropertyIndex::Exists(key, entry.value_, entry.record_);
        });
  }

  /**
   * @brief - Returns vector of keys of label-property indice.
   */
  std::vector<Key> GetIndicesKeys() {
    std::vector<Key> indices;
    for (auto index : indices_.access()) {
      indices.push_back(index.first);
    }
    return indices;
  }

 private:
  /**
   * @brief - Contains value, vlist and vertex record to distinguish between
   * index entries.
   */
  class IndexEntry : public TotalOrdering<IndexEntry> {
   public:
    IndexEntry(const IndexEntry &entry, const Vertex *new_record)
        : IndexEntry(entry.value_, entry.vlist_, new_record) {}
    IndexEntry(const PropertyValue &value, mvcc::VersionList<Vertex> *vlist,
               const Vertex *record)
        : value_(value), vlist_(vlist), record_(record) {}

    // Comparision operators - we need them to keep this sorted inside
    // skiplist.
    bool operator<(const IndexEntry &other) const {
      bool this_value_smaller = Less(this->value_, other.value_);
      if (this_value_smaller || Less(other.value_, this->value_))
        return this_value_smaller;
      if (this->vlist_ != other.vlist_) return this->vlist_ < other.vlist_;
      return this->record_ < other.record_;
    }

    bool operator==(const IndexEntry &other) const {
      return !(*this < other) && !(other < *this);
    }

    /**
     * @brief - For two property values - orders the records by type and then by
     * value. Except for integers and doubles - those are both converted to
     * double and then compared.
     * @return true if the first property value is smaller( should be before)
     * than the second one
     */
    static bool Less(const PropertyValue &a, const PropertyValue &b) {
      if (a.type() != b.type() &&
          !(IsCastableToDouble(a) && IsCastableToDouble(b)))
        return a.type() < b.type();

      if (a.type() == b.type()) {
        switch (a.type()) {
          case PropertyValue::Type::Null:
            return false;
          case PropertyValue::Type::String:
            return a.Value<std::string>() < b.Value<std::string>();
          case PropertyValue::Type::Bool:
            return a.Value<bool>() < b.Value<bool>();
          case PropertyValue::Type::Int:
            return a.Value<int64_t>() < b.Value<int64_t>();
          case PropertyValue::Type::Double:
            return a.Value<double>() < b.Value<double>();
          case PropertyValue::Type::List: {
            auto va = a.Value<std::vector<PropertyValue>>();
            auto vb = b.Value<std::vector<PropertyValue>>();
            if (va.size() != vb.size()) return va.size() < vb.size();
            return lexicographical_compare(va.begin(), va.end(), vb.begin(),
                                           vb.end(), Less);
          }
          default:
            permanent_fail("Unimplemented type operator.");
        }
      }

      // Types are int and double - convert int to double
      return GetDouble(a) < GetDouble(b);
    }

    /**
     * @brief - Return value casted to double. This is only possible for
     * integers and doubles.
     */
    static double GetDouble(const PropertyValue &value) {
      debug_assert(value.type() == PropertyValue::Type::Int ||
                       value.type() == PropertyValue::Type::Double,
                   "Invalid data type.");
      if (value.type() == PropertyValue::Type::Int)
        return static_cast<double>(value.Value<int64_t>());
      return value.Value<double>();
    }

    /**
     * @brief - Return if this value is castable to double (returns true for
     * integers and doubles).
     */
    static bool IsCastableToDouble(const PropertyValue &value) {
      return value.type() == PropertyValue::Type::Int ||
             value.type() == PropertyValue::Type::Double;
    }

    /**
     * @brief - Check if previous IndexEntry represents the same vlist/value
     * pair.
     * @return - true if IndexEntries are equal by the vlist/value pair.
     */
    bool IsAlreadyChecked(const IndexEntry &previous) const {
      return previous.vlist_ == this->vlist_ &&
             !Less(previous.value_, this->value_) &&
             !Less(this->value_, previous.value_);
    }

    const PropertyValue value_;
    mvcc::VersionList<Vertex> *const vlist_{nullptr};
    const Vertex *const record_{nullptr};
  };

  /**
   * @brief - Insert value, vlist, vertex into corresponding index (key) if the
   * index exists.
   * @param index - into which index to add
   * @param value - value which to add
   * @param vlist - pointer to vlist entry to add
   * @param vertex - pointer to vertex record entry to add (contained in vlist)
   */
  void Insert(SkipList<IndexEntry> &index, const PropertyValue &value,
              mvcc::VersionList<Vertex> *const vlist,
              const Vertex *const vertex) {
    index.access().insert(IndexEntry(value, vlist, vertex));
  }

  /**
   * @brief - Get storage for this key.
   * @param key - Label and and property for which to query.
   * @return pointer to skiplist of IndexEntries, if none which matches key
   * exists return nullptr
   */
  SkipList<IndexEntry> *GetKeyStorage(const Key &key) {
    auto access = indices_.access();
    auto iter = access.find(key);
    if (iter == access.end()) return nullptr;
    return iter->second;
  }

  /**
   * @brief - Check if Vertex contains label and property with the given
   * value.
   * @param key - label and property to check for.
   * @param value - value of property to compare
   * @return true if it contains, false otherwise.
   */
  static bool Exists(const Key &key, const PropertyValue &value,
                     const Vertex *const v) {
    debug_assert(v != nullptr, "Vertex is nullptr.");
    // We have to check for existance of label because the transaction
    // might not see the label, or the label was deleted and not yet
    // removed from the index.
    const auto &labels = v->labels_;
    if (std::find(labels.begin(), labels.end(), key.label_) == labels.end())
      return false;
    auto prop = v->properties_.at(key.property_);
    // Property doesn't exists.
    if (prop.type() == PropertyValue::Type::Null) return false;
    // Property value is the same as expected.
    return !IndexEntry::Less(prop, value) && !IndexEntry::Less(value, prop);
  }

  ConcurrentMap<Key, SkipList<IndexEntry> *> indices_;
  ConcurrentSet<Key> ready_for_use_;
};
