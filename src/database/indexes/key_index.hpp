#pragma once

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_datatypes.hpp"
#include "database/indexes/index_utils.hpp"
#include "mvcc/version_list.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "transactions/transaction.hpp"
#include "utils/total_ordering.hpp"

/**
 * @brief Implements index update and acquire.
 * @Tparam TKey - underlying type by which to key objects
 * @Tparam TRecord - object stored under the given key
 */
template <typename TKey, typename TRecord>
class KeyIndex {
 public:
  KeyIndex(){};
  KeyIndex(const KeyIndex &other) = delete;
  KeyIndex(KeyIndex &&other) = delete;
  KeyIndex &operator=(const KeyIndex &other) = delete;
  KeyIndex &operator=(KeyIndex &&other) = delete;
  /**
   * @brief - Clear all indexes so that we don't leak memory.
   */
  ~KeyIndex() {
    for (auto key_indices_pair : indices_.access()) {
      // Delete skiplist because we created it with a new operator.
      delete key_indices_pair.second;
    }
  }
  /**
   * @brief - Add record, vlist, if new, to TKey specific storage.
   * @param key - TKey index to update.
   * @param vlist - pointer to vlist entry to add
   * @param record - pointer to record entry to add (contained in vlist)
   */
  void Update(const TKey &key, mvcc::VersionList<TRecord> *vlist,
              const TRecord *const record) {
    GetKeyStorage(key)->access().insert(IndexEntry(vlist, record));
  }

  /**
   * @brief - Get all the inserted vlists in TKey specific storage which
   * still have that label visible in this transaction.
   * @param key - key to query.
   * @param t - current transaction, which determines visibility.
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   * @return iterable collection of vlists records<TRecord> with the requested
   * TKey.
   */
  auto GetVlists(const TKey &key, tx::Transaction &t,
                 bool current_state = false) {
    return IndexUtils::GetVlists<IndexEntry, TRecord>(
        *GetKeyStorage(key), t,
        [this, key](const IndexEntry &, const TRecord *record) {
          return Exists(key, record);
        },
        current_state);
  }

  /**
   * @brief - Return number of items in skiplist associated with the given
   * TKey. This number could be imprecise because of the underlying skiplist
   * storage. Use this as a hint, and not as a rule.
   * Moreover, some transaction probably sees only part of the skiplist since
   * not all versions are visible for it. Also, garbage collection might now
   * have been run for some time so the index might have accumulated garbage.
   * @param key - key to query for.
   * @return number of items
   */
  auto Count(const TKey &key) { return GetKeyStorage(key)->access().size(); }

  /**
   * @brief - Removes from the index all entries for which records don't contain
   * the given label anymore. Update all record which are not visible for any
   * transaction with an id larger or equal to `id`.
   * @param id - oldest active id, safe to remove everything deleted before this
   * id.
   * @param engine - transaction engine to see which records are commited
   */
  void Refresh(const Id &id, tx::Engine &engine) {
    return IndexUtils::Refresh<TKey, IndexEntry, TRecord>(
        indices_, id, engine, [this](const TKey &key, const IndexEntry &entry) {
          return Exists(key, entry.record_);
        });
  }

 private:
  /**
   * @brief - Contains vlist and record pointers.
   */
  class IndexEntry : public TotalOrdering<IndexEntry> {
   public:
    IndexEntry(const IndexEntry &entry, const TRecord *const new_record)
        : IndexEntry(entry.vlist_, new_record) {}
    IndexEntry(mvcc::VersionList<TRecord> *const vlist,
               const TRecord *const record)
        : vlist_(vlist), record_(record) {}

    // Comparision operators - we need them to keep this sorted inside
    // skiplist.
    // This needs to be sorted first by vlist and second record because we
    // want to keep same vlists close together since we need to filter them to
    // get only the unique ones.
    bool operator<(const IndexEntry &other) const {
      if (this->vlist_ != other.vlist_) return this->vlist_ < other.vlist_;
      return this->record_ < other.record_;
    }

    bool operator==(const IndexEntry &other) const {
      return this->vlist_ == other.vlist_ && this->record_ == other.record_;
    }

    /**
     * @brief - Checks if previous IndexEntry has the same vlist as this
     * IndexEntry.
     * @return - true if the vlists match.
     */
    bool IsAlreadyChecked(const IndexEntry &previous) const {
      return previous.vlist_ == this->vlist_;
    }

    mvcc::VersionList<TRecord> *const vlist_;
    const TRecord *const record_;
  };

  /**
   * @brief - Get storage for this label. Creates new
   * storage if this key is not yet indexed.
   * @param key - key for which to access storage.
   * @return pointer to skiplist of version list records<T>.
   */
  auto GetKeyStorage(const TKey &key) {
    auto access = indices_.access();
    // Avoid excessive new/delete by first checking if it exists.
    auto iter = access.find(key);
    if (iter == access.end()) {
      auto skiplist = new SkipList<IndexEntry>;
      auto ret = access.insert(key, skiplist);
      // In case some other insert managed to create new skiplist we shouldn't
      // leak memory and should delete this one accordingly.
      if (ret.second == false) delete skiplist;
      return ret.first->second;
    }
    return iter->second;
  }

  /**
   * @brief - Check if Vertex contains label.
   * @param label - label to check for.
   * @return true if it contains, false otherwise.
   */
  bool Exists(const GraphDbTypes::Label &label, const Vertex *const v) const {
    debug_assert(v != nullptr, "Vertex is nullptr.");
    // We have to check for existance of label because the transaction
    // might not see the label, or the label was deleted and not yet
    // removed from the index.
    const auto &labels = v->labels_;
    return std::find(labels.begin(), labels.end(), label) != labels.end();
  }

  /**
   * @brief - Check if Edge has edge_type.
   * @param edge_type - edge_type to check for.
   * @return true if it has that edge_type, false otherwise.
   */
  bool Exists(const GraphDbTypes::EdgeType &edge_type,
              const Edge *const e) const {
    debug_assert(e != nullptr, "Edge is nullptr.");
    // We have to check for equality of edge types because the transaction
    // might not see the edge type, or the edge type was deleted and not yet
    // removed from the index.
    return e->edge_type_ == edge_type;
  }

  ConcurrentMap<TKey, SkipList<IndexEntry> *> indices_;
};
