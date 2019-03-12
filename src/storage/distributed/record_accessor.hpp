/// @file
#pragma once

#include <glog/logging.h>

#include "distributed/cached_record_data.hpp"
#include "storage/common/types/property_value.hpp"
#include "storage/common/types/property_value_store.hpp"
#include "storage/common/types/types.hpp"
#include "storage/distributed/address.hpp"
#include "storage/distributed/gid.hpp"
#include "storage/distributed/mvcc/version_list.hpp"

namespace database {
class GraphDbAccessor;
struct StateDelta;
};  // namespace database

/**
 * An accessor to a database record (an Edge or a Vertex).
 *
 * Exposes view and update functions to the client programmer.
 * Assumes responsibility of doing all the relevant book-keeping
 * (such as index updates etc).
 *
 * @tparam TRecord Type of record (MVCC Version) of the accessor.
 */
template <typename TRecord>
class RecordAccessor {
 public:
  using AddressT = storage::Address<mvcc::VersionList<TRecord>>;

  // this class is default copyable, movable and assignable
  RecordAccessor(const RecordAccessor &other);
  RecordAccessor(RecordAccessor &&other) = delete;
  RecordAccessor &operator=(const RecordAccessor &other);
  RecordAccessor &operator=(RecordAccessor &&other) = delete;

 protected:
  /**
   * Protected destructor because we allow inheritance, but nobody should own a
   * pointer to plain RecordAccessor.
   */
  ~RecordAccessor();

  /**
   * Only derived types may allow construction.
   *
   * @param address Address (local or global) of the Vertex/Edge of this
   * accessor.
   * @param db_accessor The DB accessor that "owns" this record accessor.
   * @param impl Borrowed pointer to the underlying implementation.
   */
  RecordAccessor(AddressT address, database::GraphDbAccessor &db_accessor);

 public:
  /** Gets the property for the given key. */
  PropertyValue PropsAt(storage::Property key) const;

  /** Sets a value on the record for the given property. */
  void PropsSet(storage::Property key, PropertyValue value);

  /** Erases the property for the given key. */
  void PropsErase(storage::Property key);

  /** Removes all the properties from this record. */
  void PropsClear();

  /** Returns the properties of this record. */
  PropertyValueStore Properties() const;

  bool operator==(const RecordAccessor &other) const;

  bool operator!=(const RecordAccessor &other) const {
    return !(*this == other);
  }

  /** Returns a GraphDB accessor of this record accessor. */
  database::GraphDbAccessor &db_accessor() const;

  /**
   * Returns a globally-unique ID of this vertex or edge. Note that vertices
   * and edges have separate ID domains, there can be a vertex with ID X and an
   * edge with the same id.
   */
  gid::Gid gid() const;

  AddressT address() const;

  AddressT GlobalAddress() const;

  /*
   * Switches this record accessor to use the latest version visible to the
   * current transaction+command.  Possibly the one that was created by this
   * transaction+command.
   *
   * @return A reference to this.
   */
  RecordAccessor<TRecord> &SwitchNew();

  /** Returns the new record pointer. */
  TRecord *GetNew() const;

  /**
   * Attempts to switch this accessor to use the latest version not updated by
   * the current transaction+command.  If that is not possible (vertex/edge was
   * created by the current transaction/command), it does nothing (current
   * remains pointing to the new version).
   *
   * @return A reference to this.
   */
  RecordAccessor<TRecord> &SwitchOld();

  /** Returns the old record pointer. */
  TRecord *GetOld() const;

  /**
   * Reconstructs the internal state of the record accessor so it uses the
   * versions appropriate to this transaction+command.
   *
   * @return True if this accessor is valid after reconstruction.  This means
   * that at least one record pointer was found (either new_ or old_), possibly
   * both.
   */
  bool Reconstruct() const;

  /**
   * Ensures there is an updateable version of the record in the version_list,
   * and that the `new_` pointer points to it. Returns a reference to that
   * version.
   *
   * It is not legal to call this function on a Vertex/Edge that has been
   * deleted in the current transaction+command.
   *
   * @throws RecordDeletedError
   */
  void update() const;

  /**
   * Returns true if the given accessor is visible to the given transaction.
   *
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   */
  bool Visible(const tx::Transaction &t, bool current_state) const;

  // TODO: This shouldn't be here, because it's only relevant in distributed.
  /** Indicates if this accessor represents a local Vertex/Edge, or one whose
   * owner is some other worker in a distributed system. */
  bool is_local() const { return address_.is_local(); }

  /**
   * Returns Cypher Id of this record.
   */
  int64_t CypherId() const;

  /**
   * If accessor holds remote record, this method will hold remote data until
   * released. This is needed for methods that return pointers.
   * This method can be called multiple times.
   */
  void HoldCachedData() const;

  /**
   * If accessor holds remote record, this method will release remote data.
   * This is needed for methods that return pointers.
   */
  void ReleaseCachedData() const;

  /** Returns the current version (either new_ or old_) set on this
   * RecordAccessor. */
  TRecord *GetCurrent() const;

 protected:
  /**
   * The database::GraphDbAccessor is friend to this accessor so it can
   * operate on it's data (mvcc version-list and the record itself).
   * This is legitimate because database::GraphDbAccessor creates
   * RecordAccessors
   * and is semantically their parent/owner. It is necessary because
   * the database::GraphDbAccessor handles insertions and deletions, and these
   * operations modify data intensively.
   */
  friend database::GraphDbAccessor;

  /** Process a change delta, e.g. by writing WAL. */
  void ProcessDelta(const database::StateDelta &delta) const;

  void SendDelta(const database::StateDelta &delta) const;

 private:
  enum class CurrentRecord : bool { OLD, NEW };

  struct Local {
    /**
     * Version that has been modified (created or updated) by the current
     * transaction+command.
     *
     * Can be null when the record has not been modified in the current
     * transaction+command. It is also possible that the modification
     * has happened, but this RecordAccessor does not know this. To
     * ensure correctness, the `SwitchNew` function must check if this
     * is null, and if it is it must check with the vlist_ if there is
     * an update.
     */
    TRecord *newr{nullptr};
    /**
     * Latest version which is visible to the current transaction+command
     * but has not been created nor modified by the current transaction+command.
     *
     * Can be null only when the record itself (the version-list) has
     * been created by the current transaction+command.
     */
    TRecord *old{nullptr};
  };

  struct Remote {
    /* TODO (vkasljevic) possible improvement (to discuss)
     * change to std::unique_ptr and borrow it from data manager
     * and later return it to data manager
     */
    std::shared_ptr<distributed::CachedRecordData<TRecord>> data;

    /* Keeps track of how many times HoldRemoteData was called. */
    unsigned short lock_counter{0};

    /* Has Update() been called. This is needed because Update() method creates
     * new record if it doesn't exist. If that record is evicted from cache
     * and fetched again it wont have a new record.
     */
    bool has_updated{false};
  };

  // The database accessor for which this record accessor is created
  // Provides means of getting to the transaction and database functions.
  // Immutable, set in the constructor and never changed.
  database::GraphDbAccessor *db_accessor_;
  AddressT address_;

  union {
    Local local_;
    Remote remote_;
  };

  mutable CurrentRecord current_ = CurrentRecord::OLD;

  /**
   * Flag that indicates whether Reconstruct() was called.
   * This is needed because edges are lazy initialized.
   */
  mutable bool is_initialized_ = false;
};

/** Error when trying to update a deleted record */
class RecordDeletedError : public utils::BasicException {
 public:
  RecordDeletedError()
      : utils::BasicException(
            "Can't update a record deleted in the current transaction+commad") {
  }
};
