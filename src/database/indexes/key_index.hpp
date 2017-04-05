#pragma once

#include "cppitertools/filter.hpp"
#include "cppitertools/imap.hpp"

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_datatypes.hpp"
#include "mvcc/version_list.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"
#include "transactions/transaction.hpp"

/**
 * @brief Implements index update and acquire.
 * @Tparam TKey - underlying type by which to key objects
 * @Tparam TRecord - object stored under the given key
 */
template <typename TKey, typename TRecord>
class KeyIndex {
 public:
  /**
   * @brief - Add vlist, if new, to TKey specific storage.
   * @param key - TKey index to update.
   * @param vlist - pointer to vlist entry to add.
   */
  void Update(const TKey &key, mvcc::VersionList<TRecord> *vlist) {
    GetKeyStorage(key)->access().insert(vlist);
  }

  /**
   * @brief - Acquire all the inserted vlists in TKey specific storage which
   * still have that label visible in this transaction.
   * @param key - key to query.
   * @param t - current transaction, which determines visibility.
   * @return iterable collection of vlists records<TRecord> with the requested
   * TKey.
   */
  auto Acquire(const TKey &key, const tx::Transaction &t) {
    auto index = GetKeyStorage(key);
    return iter::filter(
        [this, &key, &t](auto vlist) {
          auto version = vlist->find(t);
          if (version == nullptr) return false;
          return Exists(key, version);
        },
        index->access());
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

 private:
  /**
   * @brief - Get storage for this label. Creates new
   * storage if this label is not yet indexed.
   * @param label - Label for which to access storage.
   * @return pointer to skiplist of version list records<T>.
   */
  auto GetKeyStorage(const TKey &key) {
    auto access = index_.access();
    // Avoid excessive new/delete by first checking if it exists.
    auto iter = access.find(key);
    if (iter == access.end()) {
      auto skiplist = new SkipList<mvcc::VersionList<TRecord> *>;
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
  bool Exists(const GraphDbTypes::Label &label, const Vertex *v) const {
    // We have to check for existance of label because the transaction
    // might not see the label, or the label was deleted and not yet
    // removed from the index.
    auto labels = v->labels_;
    return std::find(labels.begin(), labels.end(), label) != labels.end();
  }

  /**
   * @brief - Check if Edge has edge_type.
   * @param edge_type - edge_type to check for.
   * @return true if it has that edge_type, false otherwise.
   */
  bool Exists(const GraphDbTypes::EdgeType &edge_type, const Edge *e) const {
    // We have to check for equality of edge types because the transaction
    // might not see the edge type, or the edge type was deleted and not yet
    // removed from the index.
    return e->edge_type_ == edge_type;
  }

  ConcurrentMap<TKey, SkipList<mvcc::VersionList<TRecord> *> *> index_;
};
