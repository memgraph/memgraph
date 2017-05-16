#pragma once

#include "cppitertools/filter.hpp"
#include "cppitertools/imap.hpp"

#include "data_structures/concurrent/concurrent_map.hpp"
#include "mvcc/version_list.hpp"
#include "transactions/transaction.hpp"

namespace IndexUtils {
/**
 * @brief - Get all inserted vlists in TKey specific storage which
 * still return true for the 'exists' function.
 * @param index - index from which to get vlists
 * @param t - current transaction, which determines visibility.
 * @param exists - method which determines visibility of entry and version
 * (record) of the underlying objects (vertex/edge)
 * @param current_state If true then the graph state for the
 *    current transaction+command is returned (insertions, updates and
 *    deletions performed in the current transaction+command are not
 *    ignored).
 * @Tparam TIndexEntry - index entry inside skiplist
 * @Tparam TRecord - type of record under index (edge/vertex usually.)
 * @return iterable collection of distinct vlist records<TRecord> for which
 * exists function evaluates as true
 */
template <class TIndexEntry, class TRecord>
static auto GetVlists(
    SkipList<TIndexEntry> &index, const tx::Transaction &t,
    const std::function<bool(const TIndexEntry &, const TRecord *)> exists,
    bool current_state = false) {
  TIndexEntry *prev = nullptr;
  auto filtered = iter::filter(
      [&t, exists, prev, current_state](TIndexEntry &entry) mutable {
        // Check if the current entry could offer new possible return value
        // with respect to the previous entry we evaluated.
        // We do this to guarantee uniqueness, and also as an optimization to
        // avoid checking same vlist twice when we can.
        if (prev && entry.IsAlreadyChecked(*prev)) return false;
        prev = &entry;

        // TODO when refactoring MVCC reconsider the return-value-arg idiom
        // here
        TRecord *old_record, *new_record;
        entry.vlist_->find_set_old_new(t, old_record, new_record);
        // filtering out records not visible to the current
        // transaction+command
        // taking into account the current_state flag
        bool visible =
            (old_record && !(current_state && old_record->is_deleted_by(t))) ||
            (current_state && new_record && !new_record->is_deleted_by(t));
        if (!visible) return false;
        // if current_state is true and we have the new record, then that's
        // the reference value, and that needs to be compared with the index
        // predicate

        return (current_state && new_record) ? exists(entry, new_record)
                                             : exists(entry, old_record);
      },
      index.access());
  return iter::imap([](auto entry) { return entry.vlist_; },
                    std::move(filtered));
}

/**
 * @brief - Removes from the index all entries for which records don't contain
 * the given label/edge type/label + property anymore. Also update (remove)
 * all record which are not visible for any transaction with an id larger or
 * equal to `id`. This method assumes that the MVCC GC has been run with the
 * same 'id'.
 * @param indices - map of index entries (TIndexKey, skiplist<TIndexEntry>)
 * @param id - oldest active id, safe to remove everything deleted before this
 * id.
 * @param engine - transaction engine to see which records are commited
 * @param exists - function which checks 'key' and 'entry' if the entry still
 * contains required properties (key + optional value (in case of label_property
 * index))
 * @Tparam Tkey - index key
 * @Tparam TIndexEntry - index entry inside skiplist
 * @Tparam TRecord - type of record under index (edge/vertex usually.)
 */
template <class TKey, class TIndexEntry, class TRecord>
static void Refresh(
    ConcurrentMap<TKey, SkipList<TIndexEntry> *> &indices, const Id &id,
    tx::Engine &engine,
    const std::function<bool(const TKey &, const TIndexEntry &)> exists) {
  for (auto &key_indices_pair : indices.access()) {
    auto indices_entries_accessor = key_indices_pair.second->access();
    for (auto indices_entry : indices_entries_accessor) {
      // Remove it from index if it's deleted before the current id, or it
      // doesn't have that label/edge_type/label+property anymore. We need to
      // be careful when we are deleting the record which is not visible
      // anymore because even though that record is not visible, some other,
      // newer record could be visible, and might contain the label, and might
      // not be in the index - that's why we can't remove it completely, but
      // just update it's record value.
      // We also need to be extra careful when checking for existance of
      // label/edge_type because that record could still be under the update
      // operation and as such could be modified while we are reading the
      // label/edge_type - that's why we have to wait for it's creation id to
      // be lower than ours `id` as that means it's surely either commited or
      // aborted as we always refresh with the oldest active id.
      if (indices_entry.record_->is_not_visible_from(id, engine)) {
        // We first have to insert and then remove, because otherwise we might
        // have a situation where there exists a vlist with the record
        // containg the label/edge_type/label+property but it's not in our
        // index.
        // Get the oldest not deleted version for current 'id' (MVCC GC takes
        // care of this.)
        auto new_record = indices_entry.vlist_->Oldest();
        if (new_record != nullptr)
          indices_entries_accessor.insert(
              TIndexEntry(indices_entry, new_record));

        auto success = indices_entries_accessor.remove(indices_entry);
        debug_assert(success == true, "Not able to delete entry.");
      } else if (indices_entry.record_->tx.cre() < id &&
                 !exists(key_indices_pair.first, indices_entry)) {
        // Since id is the oldest active id, if the record has been created
        // before it we are sure that it won't be modified anymore and that
        // the creating transaction finished, that's why it's safe to check
        // it, and potentially remove it from index.
        indices_entries_accessor.remove(indices_entry);
      }
    }
  }
}
};  // IndexUtils
