# Label Indexes

These are unsorted indexes that contain all the vertices that have the label
the indexes are for (one index per label). These kinds of indexes get
automatically generated for each label used in the database.

### Updating the Indexes

Whenever something gets added to the record we update the index (add that
record to index). We keep an index which might contain garbage (not relevant
records, because the value got removed or something similar) but we will
filter it out when querying the index. We do it like this because we don't
have to do bookkeeping and deciding if we update the index on the end of the
transaction (commit/abort phase), moreover current interpreter advances the
command in transaction and as such assumes that the indexes now contain
objects added in the previous command inside this transaction, so we need to
update over the whole scope of transaction (whenever something is added to the
record).

### Index Entries Label

These kinds of indexes are internally keeping track of pair (record, vlist).
Why do we need to keep track of exactly those two things?

Problems with two different approaches

1) Keep track of just the record:

  - We need the `VersionList` for creating an accessor (this in itself is a
    deal-breaker).
  - Semantically it makes sense. An edge/vertex maps bijectionally to a
    `VersionList`.
  - We might try to access some members of record while the record is being
    modified from another thread.
  - A vertex/edge could get updated, thus expiring the record in the index.
    The newly created record should be present in the index, but it's not.
    Without the `VersionList` we can't reach the newly created record.
  - Probably there are even more reasons... It should be obvious by now that
    we need the `VersionList` in the index.

2) Keep track of just the version list:

  - Removing from an index is a problem for two major reasons. First, if we
    only have the `VersionList`, checking if it should be removed implies
    checking all the reachable records, which is not thread-safe. Second,
    there are issues with concurrent removal and insertion. The cleanup thread
    could determine the vertex/edge should be removed from the index and
    remove it, while in between those ops another thread attempts to insert
    the `VersionList` into the index. The insertion does nothing because the
    `VersionList` is already in, but it gets removed immediately after.

Because of inability to keep track of just the record, or value, we need to
keep track of both of them.  Resolution of problems mentioned above, in the
same order, with (record, vlist) pair

  - simple `vlist.find(current transaction)` will get us the newest visible
    record
  - we'll never try to access some record if it's still being written since we
    will always operate on vlist.find returned record
  - newest record will contain that label
  - since we have (record, vlist) pair as the key in the index when we update
    and delete in the same time we will never delete the same record, vlist
    pair we are adding because the record, vlist pair we are deleting is
    already superseded by a newer record and as such won't be inserted while
    it's being deleted

### Querying the Index

We run through the index for the given label and do `vlist.find` operation for
the current transaction, and check if the newest return record has that
label. If it has it then we return it. By now you are probably wondering
aren't we sometimes returning duplicate vlist entries? And you are wondering
correctly, we would be returning them, but we are making sure that the entires
in the index are sorted by their `vlist*` and as such we can filter consecutive
duplicate `vlist*` to only return one of those while still being able to create
an iterator to index.

### Cleaning the Index

Cleaning the index is not as straightforward as it seems as a lot of garbage
can accumulate, but it's hard to know when exactly can we delete some (record,
vlist) pair. First, let's assume that we are doing the cleaning process at
some `transaction_id`, `id` such that there doesn't exist an active transaction
with an id lower than `id`.

We scan through the whole index and for each (record, vlist) pair we first
check if it was deleted before the id (i.e. no transaction with an id >= `id`
will ever again see that record), if it was deleted before we might naively
say that it's safe to delete it, but, we must take into account that when some
new record is created from this record (update operation), that record still
contains the label but by deleting this record we won't be able to see that
vlist because that new record won't add again to index because we didn't
explicitly add that label again to it.

Because of this we have to 'update' this index (record, vlist) pair. We have
to update the record to now point to a newer record in vlist, the one that is
not deleted yet. We can do that by querying the `version_list` for the last
record inside (oldest it has &mdash; remember that `mvcc_gc` will re-link not
visible records so the last record will be visible for the current GC id).
When updating the record inside the index, it's not okay to just update the
pointer and leave the index as it is, because with updating the `record*` we
might change the relative order of entries inside the index. We first have to
re-insert it with new `record*`, and then delete the old entry. And we need to
do insertion before the remove operation! Otherwise it could happen that the
vlist with a newer record with that label won't exist while some transaction
is querying the index.

Records which we added as a consequence of deleting older records will be
eventually removed from the index if they don't contain label because if we
see that the record is not deleted we try to check if that record still
contains the label. We also need to be careful here because we can't check
that while the record is being potentially updated by some transaction (race
condition), so we need can check if records still contain label if it's
creation id is smaller than our `id`, as that implies that the creating
transaction either aborted or committed as our `id` is equal to the oldest
active transaction in time of starting the GC.
