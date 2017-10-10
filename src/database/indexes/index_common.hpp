#pragma once

#include "cppitertools/filter.hpp"
#include "cppitertools/imap.hpp"
#include "cppitertools/takewhile.hpp"

#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/concurrent/skiplist.hpp"
#include "mvcc/version_list.hpp"
#include "transactions/transaction.hpp"

namespace IndexUtils {
/**
 * @brief - Wrap beginning iterator to iterable object. This provides us with
 * begin and end iterator, and allows us to iterate from the iterator given in
 * constructor till the end of the collection over which we are really
 * iterating, i.e. it allows us to iterate over the suffix of some skiplist
 * hence the name SkipListSuffix.
 */
template <class TIterator, class TValue, typename TAccessor>
class SkipListSuffix {
 public:
  class Iterator {
   public:
    Iterator(TIterator current) : current_(current) {}

    TValue &operator*() { return *current_; }

    bool operator!=(Iterator other) const {
      return this->current_ != other.current_;
    }

    Iterator &operator++() {
      ++current_;
      return *this;
    }

   private:
    TIterator current_;
  };

  SkipListSuffix(
      const TIterator begin,
      typename SkipList<TValue>::template Accessor<TAccessor> &&accessor)
      : begin_(begin), accessor_(std::move(accessor)) {}

  Iterator begin() const { return Iterator(begin_); }
  Iterator end() { return Iterator(accessor_.end()); }

  TIterator begin_;
  typename SkipList<TValue>::template Accessor<TAccessor> accessor_;
};

/**
 * @brief - Get all inserted vlists in TKey specific storage which
 * still return true for the 'exists' function.
 * @param skiplist_accessor - accessor used to get begin iterator, and that
 * should be used to get end iterator as well.
 * @param begin - starting iterator for vlist iteration.
 * @param predicate - function which checks if TIndexEntry has a value that we
 * are looking for
 * @param t - current transaction, which determines visibility.
 * @param exists - method which determines visibility of entry and version
 * (record) of the underlying objects (vertex/edge)
 * @param current_state If true then the graph state for the
 *    current transaction+command is returned (insertions, updates and
 *    deletions performed in the current transaction+command are not
 *    ignored).
 * @Tparam TIndexEntry - index entry inside skiplist
 * @Tparam TRecord - type of record under index (edge/vertex usually.)
 * @Tparam TAccessor - type of accessor to use (const skiplist/non const
 * skiplist).
 * @return iterable collection of distinct vlist records<TRecord> for which
 * exists function evaluates as true
 */
template <class TIterator, class TIndexEntry, class TRecord, typename TAccessor>
static auto GetVlists(
    typename SkipList<TIndexEntry>::template Accessor<TAccessor>
        &&skiplist_accessor,
    TIterator begin,
    const std::function<bool(const TIndexEntry &entry)> predicate,
    const tx::Transaction &t,
    const std::function<bool(const TIndexEntry &, const TRecord *)> exists,
    bool current_state = false) {
  TIndexEntry *prev = nullptr;
  auto range = iter::takewhile(
      predicate, SkipListSuffix<TIterator, TIndexEntry, TAccessor>(
                     begin, std::move(skiplist_accessor)));
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
            (old_record && !(current_state && old_record->is_expired_by(t))) ||
            (current_state && new_record && !new_record->is_expired_by(t));
        if (!visible) return false;
        // if current_state is true and we have the new record, then that's
        // the reference value, and that needs to be compared with the index
        // predicate

        return (current_state && new_record) ? exists(entry, new_record)
                                             : exists(entry, old_record);
      },
      std::move(range));
  return iter::imap([](auto entry) { return entry.vlist_; },
                    std::move(filtered));
}

/**
 * @brief - Removes from the index all entries for which records don't contain
 * the given label/edge type/label + property anymore. Also update (remove)
 * all records which are not visible for any transaction in the given
 * 'snapshot'. This method assumes that the MVCC GC has been run with the
 * same 'snapshot'.
 *
 * @param indices - map of index entries (TIndexKey, skiplist<TIndexEntry>)
 * @param snapshot - the GC snapshot. Consists of the oldest active
 * transaction's snapshot, with that transaction's id appened as last.
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
    ConcurrentMap<TKey, SkipList<TIndexEntry> *> &indices,
    const tx::Snapshot &snapshot, tx::Engine &engine,
    const std::function<bool(const TKey &, const TIndexEntry &)> exists) {
  // iterate over all the indices
  for (auto &key_indices_pair : indices.access()) {
    // iterate over index entries
    auto indices_entries_accessor = key_indices_pair.second->access();
    for (auto indices_entry : indices_entries_accessor) {
      if (indices_entry.record_->is_not_visible_from(snapshot, engine)) {
        // be careful when deleting the record which is not visible anymore.
        // it's newer copy could be visible, and might still logically belong to
        // index (it satisfies the `exists` function).  that's why we can't just
        // remove the index entry, but also re-insert the oldest visible record
        // to the index. if that record does not satisfy `exists`, it will be
        // cleaned up in the next Refresh first insert and then remove,
        // otherwise there is a timeframe during which the record is not present
        // in the index
        auto new_record = indices_entry.vlist_->Oldest();
        if (new_record != nullptr)
          indices_entries_accessor.insert(
              TIndexEntry(indices_entry, new_record));

        [[gnu::unused]] auto success =
            indices_entries_accessor.remove(indices_entry);
        debug_assert(success, "Unable to delete entry.");
      }

      // if the record is still visible,
      // check if it satisfies the `exists` function. if not
      // it does not belong in index anymore.
      // be careful when using the `exists` function
      // because it's creator transaction could still be modifying it,
      // and modify+read is not thread-safe. for that reason we need to
      // first see if the the transaction that created it has ended
      // (tx().cre < oldest active trancsation).
      else if (indices_entry.record_->tx().cre < snapshot.back() &&
               !exists(key_indices_pair.first, indices_entry)) {
        indices_entries_accessor.remove(indices_entry);
      }
    }
  }
}
};  // namespace IndexUtils
