#pragma once

#include "cppitertools/filter.hpp"
#include "cppitertools/imap.hpp"

#include "data_structures/concurrent/concurrent_map.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_datatypes.hpp"
#include "mvcc/version_list.hpp"
#include "transactions/transaction.hpp"

/**
 * @brief Implements index update and acquire.
 * @Tparam T - underlying type in version list.
 */
template <typename T>
class LabelsIndex {
 public:
  /**
   * @brief - Add vlist, if new, to label specific storage.
   * @param label - label index to update.
   * @param vlist - pointer to vlist entry to add.
   */
  void Add(const GraphDbTypes::Label &label, mvcc::VersionList<T> *vlist) {
    GetLabel(label)->access().insert(vlist);
  }

  /**
   * @brief - Acquire all the inserted vlists in label specific storage which
   * still have that label visible in this transaction.
   * @param label - label to query.
   * @param t - current transaction, which determines visibility.
   * @return iterable collection of vlists records<T> with the requested label.
   */
  auto Acquire(const GraphDbTypes::Label &label, const tx::Transaction &t) {
    auto label_index = GetLabel(label);
    return iter::filter(
        [this, &label, &t](auto vlist) {
          auto vlist_head = vlist->find(t);
          if (vlist_head == nullptr) return false;
          auto labels = vlist_head->labels_;
          // We have to check for existance of label because the transaction
          // might not see the label, or the label was deleted and not yet
          // removed from the index.
          return std::find(labels.begin(), labels.end(), label) != labels.end();
        },
        label_index->access());
  }

  /**
   * @brief - Return number of items in skiplist associated with the given
   * label. This number could be imprecise because of the underlying skiplist
   * storage. Use this as a hint, and not as a rule.
   * @param label - label to query for.
   * @return number of items
   */
  auto Count(const GraphDbTypes::Label &label) {
    return GetLabel(label)->access().size();
  }

 private:
  /**
   * @brief - Get storage for this label. Creates new
   * storage if this label is not yet indexed.
   * @param label - Label for which to access storage.
   * @return pointer to skiplist of version list records<T>.
   */
  auto GetLabel(const GraphDbTypes::Label &label) {
    auto access = index_.access();
    auto iter = access.find(label);
    if (iter == access.end()) {
      auto skiplist = new SkipList<mvcc::VersionList<T> *>;
      auto ret = access.insert(label, skiplist);
      // In case some other insert managed to create new skiplist we shouldn't
      // leak memory and should delete this one accordingly.
      if (ret.second == false) delete skiplist;
      return ret.first->second;
    }
    return iter->second;
  }
  ConcurrentMap<GraphDbTypes::Label, SkipList<mvcc::VersionList<T> *> *> index_;
};
