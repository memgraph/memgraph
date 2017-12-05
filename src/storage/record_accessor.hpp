#pragma once

#include "glog/logging.h"

#include "database/graph_db_datatypes.hpp"
#include "mvcc/version_list.hpp"
#include "storage/gid.hpp"
#include "storage/property_value.hpp"
#include "storage/property_value_store.hpp"
#include "utils/total_ordering.hpp"

class GraphDbAccessor;

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
class RecordAccessor : public TotalOrdering<RecordAccessor<TRecord>> {
 public:
  /**
   * The GraphDbAccessor is friend to this accessor so it can
   * operate on it's data (mvcc version-list and the record itself).
   * This is legitemate because GraphDbAccessor creates RecordAccessors
   * and is semantically their parent/owner. It is necessary because
   * the GraphDbAccessor handles insertions and deletions, and these
   * operations modify data intensively.
   */
  friend GraphDbAccessor;

  /**
   * @param vlist MVCC record that this accessor wraps.
   * @param db_accessor The DB accessor that "owns" this record accessor.
   */
  RecordAccessor(mvcc::VersionList<TRecord> &vlist,
                 GraphDbAccessor &db_accessor);

  // this class is default copyable, movable and assignable
  RecordAccessor(const RecordAccessor &other) = default;
  RecordAccessor(RecordAccessor &&other) = default;
  RecordAccessor &operator=(const RecordAccessor &other) = default;
  RecordAccessor &operator=(RecordAccessor &&other) = default;

  /**
   * Gets the property for the given key.
   * @param key
   * @return
   */
  const PropertyValue &PropsAt(GraphDbTypes::Property key) const;

  /**
   * Sets a value on the record for the given property, operates on edge.
   *
   * @param key Property key.
   * @param value The value to set.
   */
  void PropsSet(GraphDbTypes::Property key, PropertyValue value);

  /**
   * Erases the property for the given key.
   *
   * @param key
   * @return
   */
  size_t PropsErase(GraphDbTypes::Property key);

  /**
   * Removes all the properties from this record.
   */
  void PropsClear();

  /**
   * Returns the properties of this record.
   * @return
   */
  const PropertyValueStore<GraphDbTypes::Property> &Properties() const;

  void PropertiesAccept(std::function<void(const GraphDbTypes::Property key,
                                           const PropertyValue &prop)>
                            handler,
                        std::function<void()> finish = {}) const;

  /**
   * This should be used with care as it's comparing vlist_ pointer records and
   * not actual values inside RecordAccessors.
   */
  bool operator<(const RecordAccessor &other) const {
    DCHECK(db_accessor_ == other.db_accessor_)
        << "Not in the same transaction.";
    return vlist_ < other.vlist_;
  }

  bool operator==(const RecordAccessor &other) const {
    DCHECK(db_accessor_ == other.db_accessor_)
        << "Not in the same transaction.";
    return vlist_ == other.vlist_;
  }

  /** Enables equality check against a version list pointer. This makes it
   * possible to check if an accessor and a vlist ptr represent the same graph
   * element without creating an accessor (not very cheap). */
  bool operator==(const mvcc::VersionList<TRecord> *other_vlist) const {
    return vlist_ == other_vlist;
  }

  /**
   * Returns a GraphDB accessor of this record accessor.
   *
   * @return See above.
   */
  GraphDbAccessor &db_accessor() const;

  /** Returns a database-unique index of this vertex or edge. Note that vertices
   * and edges have separate GID domains, there can be a vertex with GID X and
   * an edge with the same gid.
   */
  gid::Gid gid() const { return vlist_->gid_; }

  /*
   * Switches this record accessor to use the latest
   * version visible to the current transaction+command.
   * Possibly the one that was created by this transaction+command.
   *
   * @return A reference to this.
   */
  RecordAccessor<TRecord> &SwitchNew();

  /**
   * Attempts to switch this accessor to use the latest version not updated by
   * the current transaction+command.  If that is not possible (vertex/edge was
   * created by the current transaction/command), it does nothing (current
   * remains pointing to the new version).
   * @return A reference to this.
   */
  RecordAccessor<TRecord> &SwitchOld();

  /**
   Reconstructs the internal state of the record accessor so it uses the
   versions appropriate to this transaction+command.
   *
   @return True if this accessor is valid after reconstruction.  This means that
   at least one record pointer was found (either new_ or old_), possibly both.
   */
  bool Reconstruct() const;

 protected:
  /**
   * Ensures there is an updateable version of the record in the version_list,
   * and that the `new_` pointer points to it. Returns a reference to that
   * version.
   *
   * It is not legal to call this function on a Vertex/Edge that has been
   * deleted in the current transaction+command.
   */
  TRecord &update() const;

  /**
   * Returns the current version (either new_ or old_)
   * set on this RecordAccessor.
   *
   * @return See above.
   */
  const TRecord &current() const;

  /**
   * Pointer to the version (either old_ or new_) that READ operations
   * in the accessor should take data from. Note that WRITE operations
   * should always use new_.
   *
   * This pointer can be null if created by an accessor which lazily reads from
   * mvcc.
   */
  mutable TRecord *current_{nullptr};

  // The record (edge or vertex) this accessor provides access to.
  // Immutable, set in the constructor and never changed.
  mvcc::VersionList<TRecord> *vlist_;

 private:
  // The database accessor for which this record accessor is created
  // Provides means of getting to the transaction and database functions.
  // Immutable, set in the constructor and never changed.
  GraphDbAccessor *db_accessor_;

  /**
   * Latest version which is visible to the current transaction+command
   * but has not been created nor modified by the current transaction+command.
   *
   * Can be null only when the record itself (the version-list) has
   * been created by the current transaction+command.
   */
  mutable TRecord *old_{nullptr};

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
  mutable TRecord *new_{nullptr};
};
