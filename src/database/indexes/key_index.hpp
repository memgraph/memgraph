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
#include "utils/total_ordering.hpp"

/**
 * @brief Implements index update and acquire.
 * @Tparam TKey - underlying type by which to key objects
 * @Tparam TRecord - object stored under the given key
 */
template <typename TKey, typename TRecord>
class KeyIndex {
 public:
  /**
   * @brief - Clear all indexes so that we don't leak memory.
   */
  ~KeyIndex() {
    for (auto key_indices_pair : indices_.access())
      // Delete skiplist because we created it with a new operator.
      delete key_indices_pair.second;
  }
  /**
   * @brief - Add record, vlist, if new, to TKey specific storage.
   * @param key - TKey index to update.
   * @param vlist - pointer to vlist entry to add
   * @param record - pointer to record entry to add (contained in vlist)
   */
  void Update(const TKey &key, mvcc::VersionList<TRecord> *vlist,
              const TRecord *record) {
    GetKeyStorage(key)->access().insert(IndexEntry(vlist, record));
  }

  /**
   * @brief - Get all the inserted vlists in TKey specific storage which
   * still have that label visible in this transaction.
   * @param key - key to query.
   * @param t - current transaction, which determines visibility.
   * @return iterable collection of vlists records<TRecord> with the requested
   * TKey.
   */
  auto GetVlists(const TKey &key, tx::Transaction &t) {
    auto index = GetKeyStorage(key);
    mvcc::VersionList<TRecord> *prev = nullptr;
    auto filtered = iter::filter(
        [this, &key, &t, prev](auto entry) mutable {
          if (entry.vlist_ == prev) return false;
          auto version = entry.vlist_->find(t);
          prev = entry.vlist_;
          if (version == nullptr) return false;
          return Exists(key, version);
        },
        index->access());
    return iter::imap([this](auto entry) { return entry.vlist_; },
                      std::move(filtered));
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
    for (auto &key_indices_pair : indices_.access()) {
      auto indices_entries_accessor = key_indices_pair.second->access();
      for (auto indices_entry : indices_entries_accessor) {
        // Remove it from index if it's deleted before the current id, or it
        // doesn't have that label/edge_type anymore. We need to be careful when
        // we are deleting the record which is not visible anymore because even
        // though that record is not visible, some other, newer record could be
        // visible, and might contain the label, and might not be in the index -
        // that's why we can't remove it completely, but just update it's record
        // value.
        // We also need to be extra careful when checking for existance of
        // label/edge_type because that record could still be under the update
        // operation and as such could be modified while we are reading the
        // label/edge_type - that's why we have to wait for it's creation id to
        // be lower than ours `id` as that means it's surely either commited or
        // aborted as we always refresh with the oldest active id.
        if (indices_entry.record_->is_not_visible_from(id, engine)) {
          // We first have to insert and then remove, because otherwise we might
          // have a situation where there exists a vlist with the record
          // containg the label/edge_type but it's not in our index.
          auto new_record = indices_entry.vlist_->Oldest();
          if (new_record != nullptr)
            indices_entries_accessor.insert(
                IndexEntry(indices_entry.vlist_, new_record));
          auto success = indices_entries_accessor.remove(indices_entry);
          debug_assert(success == true, "Not able to delete entry.");
        } else if (indices_entry.record_->tx.cre() < id &&
                   !Exists(key_indices_pair.first, indices_entry.record_)) {
          indices_entries_accessor.remove(indices_entry);
        }
      }
    }
  }

 private:
  /**
   * @brief - Contains vlist and record pointers.
   */
  class IndexEntry : public TotalOrdering<IndexEntry> {
   public:
    IndexEntry(mvcc::VersionList<TRecord> *vlist, const TRecord *record)
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

    mvcc::VersionList<TRecord> *vlist_;
    const TRecord *record_;
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
  bool Exists(const GraphDbTypes::Label &label, const Vertex *v) const {
    debug_assert(v != nullptr, "Vertex is nullptr.");
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
    debug_assert(e != nullptr, "Edge is nullptr.");
    // We have to check for equality of edge types because the transaction
    // might not see the edge type, or the edge type was deleted and not yet
    // removed from the index.
    return e->edge_type_ == edge_type;
  }

  ConcurrentMap<TKey, SkipList<IndexEntry> *> indices_;
};
