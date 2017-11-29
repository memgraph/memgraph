//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 03.02.17.
//

#pragma once

#include <experimental/optional>
#include <random>

#include "cppitertools/filter.hpp"
#include "cppitertools/imap.hpp"
#include "glog/logging.h"

#include "graph_db.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "transactions/engine_master.hpp"
#include "transactions/transaction.hpp"
#include "utils/bound.hpp"

/** Thrown when creating an index which already exists. */
class IndexExistsException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/**
 * An accessor for the database object: exposes functions for operating on the
 * database. All the functions in this class should be self-sufficient: for
 * example the function for creating a new Vertex should take care of all the
 * book-keeping around the creation.
 */

class GraphDbAccessor {
  // We need to make friends with this guys since they need to access private
  // methods for updating indices.
  friend class RecordAccessor<Vertex>;
  friend class RecordAccessor<Edge>;
  friend class VertexAccessor;
  friend class EdgeAccessor;

 public:
  /**
   * Creates an accessor for the given database.
   *
   * @param db The database
   */
  explicit GraphDbAccessor(GraphDb &db);
  ~GraphDbAccessor();

  GraphDbAccessor(const GraphDbAccessor &other) = delete;
  GraphDbAccessor(GraphDbAccessor &&other) = delete;
  GraphDbAccessor &operator=(const GraphDbAccessor &other) = delete;
  GraphDbAccessor &operator=(GraphDbAccessor &&other) = delete;

  /**
   * Creates a new Vertex and returns an accessor to it. If the ID is
   * provided, the created Vertex will have that ID, and the ID counter will be
   * increased to it so collisions are avoided. This should only be used by
   * durability recovery, normal vertex creation should not provide the ID.
   *
   * You should NOT make interleaved recovery and normal DB op calls to this
   * function. Doing so will likely mess up the ID generation and crash MG.
   * Always perform recovery only once, immediately when the database is
   * created, before any transactional ops start.
   *
   * @param opt_id The desired ID. Should only be provided when recovering from
   * durability.
   * @return See above.
   */
  VertexAccessor InsertVertex(
      std::experimental::optional<int64_t> opt_id = std::experimental::nullopt);

  /**
   * Removes the vertex of the given accessor. If the vertex has any outgoing or
   * incoming edges, it is not deleted. See `DetachRemoveVertex` if you want to
   * remove a vertex regardless of connectivity.
   *
   * If the vertex has already been deleted by the current transaction+command,
   * this function will not do anything and will return true.
   *
   * @param vertex_accessor Accessor to vertex.
   * @return  If or not the vertex was deleted.
   */
  bool RemoveVertex(VertexAccessor &vertex_accessor);

  /**
   * Removes the vertex of the given accessor along with all it's outgoing
   * and incoming connections.
   *
   * @param vertex_accessor  Accessor to a vertex.
   */
  void DetachRemoveVertex(VertexAccessor &vertex_accessor);

  /**
   * Obtains the vertex for the given ID. If there is no vertex for the given
   * ID, or it's not visible to this accessor's transaction, nullopt is
   * returned.
   *
   * @param id - The ID of the sought vertex.
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   */
  std::experimental::optional<VertexAccessor> FindVertex(int64_t id,
                                                         bool current_state);

  /**
   * Returns iterable over accessors to all the vertices in the graph
   * visible to the current transaction.
   *
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   */
  auto Vertices(bool current_state) {
    DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
    // wrap version lists into accessors, which will look for visible versions
    auto accessors = iter::imap(
        [this](auto id_vlist) {
          return VertexAccessor(*id_vlist.second, *this);
        },
        db_.vertices_.access());

    // filter out the accessors not visible to the current transaction
    return iter::filter(
        [this, current_state](const VertexAccessor &accessor) {
          return Visible(accessor, current_state);
        },
        std::move(accessors));
  }

  /**
   * Return VertexAccessors which contain the current label for the current
   * transaction visibilty.
   * @param label - label for which to return VertexAccessors
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   * @return iterable collection
   */
  auto Vertices(const GraphDbTypes::Label &label, bool current_state) {
    DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
    return iter::imap(
        [this](auto vlist) { return VertexAccessor(*vlist, *this); },
        db_.labels_index_.GetVlists(label, *transaction_, current_state));
  }

  /**
   * Return VertexAccessors which contain the current label and property for the
   * given transaction visibility.
   *
   * @param label - label for which to return VertexAccessors
   * @param property - property for which to return VertexAccessors
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   * @return iterable collection
   */
  auto Vertices(const GraphDbTypes::Label &label,
                const GraphDbTypes::Property &property, bool current_state) {
    DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
    DCHECK(db_.label_property_index_.IndexExists(
        LabelPropertyIndex::Key(label, property)))
        << "Label+property index doesn't exist.";
    return iter::imap(
        [this](auto vlist) { return VertexAccessor(*vlist, *this); },
        db_.label_property_index_.GetVlists(
            LabelPropertyIndex::Key(label, property), *transaction_,
            current_state));
  }

  /**
   * Return VertexAccessors which contain the current label + property, and
   * those properties are equal to this 'value' for the given transaction
   * visibility.
   * @param label - label for which to return VertexAccessors
   * @param property - property for which to return VertexAccessors
   * @param value - property value for which to return VertexAccessors
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   * @return iterable collection
   */
  auto Vertices(const GraphDbTypes::Label &label,
                const GraphDbTypes::Property &property,
                const PropertyValue &value, bool current_state) {
    DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
    DCHECK(db_.label_property_index_.IndexExists(
        LabelPropertyIndex::Key(label, property)))
        << "Label+property index doesn't exist.";
    CHECK(value.type() != PropertyValue::Type::Null)
        << "Can't query index for propery value type null.";
    return iter::imap(
        [this](auto vlist) { return VertexAccessor(*vlist, *this); },
        db_.label_property_index_.GetVlists(
            LabelPropertyIndex::Key(label, property), value, *transaction_,
            current_state));
  }

  /**
   * Return an iterable over VertexAccessors which contain the
   * given label and whose property value (for the given property)
   * falls within the given (lower, upper) @c Bound.
   *
   * The returned iterator will only contain
   * vertices/edges whose property value is comparable with the
   * given bounds (w.r.t. type). This has implications on Cypher
   * query execuction semantics which have not been resovled yet.
   *
   * At least one of the bounds must be specified. Bonds can't be
   * @c PropertyValue::Null. If both bounds are
   * specified, their PropertyValue elments must be of comparable
   * types.
   *
   * @param label - label for which to return VertexAccessors
   * @param property - property for which to return VertexAccessors
   * @param lower - Lower bound of the interval.
   * @param upper - Upper bound of the interval.
   * @param value - property value for which to return VertexAccessors
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   * @return iterable collection of record accessors
   * satisfy the bounds and are visible to the current transaction.
   */
  auto Vertices(
      const GraphDbTypes::Label &label, const GraphDbTypes::Property &property,
      const std::experimental::optional<utils::Bound<PropertyValue>> lower,
      const std::experimental::optional<utils::Bound<PropertyValue>> upper,
      bool current_state) {
    DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
    DCHECK(db_.label_property_index_.IndexExists(
        LabelPropertyIndex::Key(label, property)))
        << "Label+property index doesn't exist.";
    return iter::imap(
        [this](auto vlist) { return VertexAccessor(*vlist, *this); },
        db_.label_property_index_.GetVlists(
            LabelPropertyIndex::Key(label, property), lower, upper,
            *transaction_, current_state));
  }

  /**
   * Creates a new Edge and returns an accessor to it. If the ID is
   * provided, the created Edge will have that ID, and the ID counter will be
   * increased to it so collisions are avoided. This should only be used by
   * durability recovery, normal edge creation should not provide the ID.
   *
   * You should NOT make interleaved recovery and normal DB op calls to this
   * function. Doing so will likely mess up the ID generation and crash MG.
   * Always perform recovery only once, immediately when the database is
   * created, before any transactional ops start.
   *
   * @param from The 'from' vertex.
   * @param to The 'to' vertex'
   * @param type Edge type.
   * @param opt_id The desired ID. Should only be provided when recovering from
   * durability.
   * @return  An accessor to the edge.
   */
  EdgeAccessor InsertEdge(
      VertexAccessor &from, VertexAccessor &to, GraphDbTypes::EdgeType type,
      std::experimental::optional<int64_t> opt_id = std::experimental::nullopt);

  /**
   * Removes an edge from the graph. Parameters can indicate if the edge should
   * be removed from data structures in vertices it connects. When removing an
   * edge both arguments should be `true`. `false` is only used when
   * detach-deleting a vertex.
   *
   * @param edge_accessor  The accessor to an edge.
   * @param remove_from_from If the edge should be removed from the its origin
   * side.
   * @param remove_from_to If the edge should be removed from the its
   * destination side.
   */
  void RemoveEdge(EdgeAccessor &edge_accessor, bool remove_from = true,
                  bool remove_to = true);

  /**
   * Obtains the edge for the given ID. If there is no edge for the given
   * ID, or it's not visible to this accessor's transaction, nullopt is
   * returned.
   *
   * @param id - The ID of the sought edge.
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   */
  std::experimental::optional<EdgeAccessor> FindEdge(int64_t id,
                                                     bool current_state);
  /**
   * Returns iterable over accessors to all the edges in the graph
   * visible to the current transaction.
   *
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   */
  auto Edges(bool current_state) {
    DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";

    // wrap version lists into accessors, which will look for visible versions
    auto accessors = iter::imap(
        [this](auto id_vlist) { return EdgeAccessor(*id_vlist.second, *this); },
        db_.edges_.access());

    // filter out the accessors not visible to the current transaction
    return iter::filter(
        [this, current_state](const EdgeAccessor &accessor) {
          return Visible(accessor, current_state);
        },
        std::move(accessors));
  }

  /**
   * Creates and returns a new accessor that represents the same graph element
   * (node / version) as the given `accessor`, but in this `GraphDbAccessor`.
   *
   * It is possible that the given `accessor` graph element is not visible in
   * this `GraphDbAccessor`'s transaction.  If that is the case, a `nullopt` is
   * returned.
   *
   * The returned accessor does NOT have the same `current_` set as the given
   * `accessor`. It has default post-construction `current_` set (`old` if
   * available, otherwise `new`).
   *
   * @param accessor The [Vertex/Edge]Accessor whose underlying graph element we
   * want in this GraphDbAccessor.
   * @return See above.
   * @tparam TAccessor Either VertexAccessor or EdgeAccessor
   */
  template <typename TAccessor>
  std::experimental::optional<TAccessor> Transfer(const TAccessor &accessor) {
    if (accessor.db_accessor_ == this)
      return std::experimental::make_optional(accessor);

    TAccessor accessor_in_this(*accessor.vlist_, *this);
    if (accessor_in_this.current_)
      return std::experimental::make_optional(std::move(accessor_in_this));
    else
      return std::experimental::nullopt;
  }

  /**
   * Adds an index for the given (label, property) and populates it with
   * existing vertices that belong to it.
   *
   * You should never call BuildIndex on a GraphDbAccessor (transaction) on
   * which new vertices have been inserted or existing ones updated. Do it
   * in a new accessor instead.
   *
   * Build index throws if an index for the given (label, property) already
   * exists (even if it's being built by a concurrent transaction and is not yet
   * ready for use).
   *
   * It also throws if there is another index being built concurrently on the
   * same database this accessor is for.
   *
   * @param label - label to build for
   * @param property - property to build for
   */
  void BuildIndex(const GraphDbTypes::Label &label,
                  const GraphDbTypes::Property &property);

  /**
   * @brief - Returns true if the given label+property index already exists and
   * is ready for use.
   */
  bool LabelPropertyIndexExists(const GraphDbTypes::Label &label,
                                const GraphDbTypes::Property &property) const {
    DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
    return db_.label_property_index_.IndexExists(
        LabelPropertyIndex::Key(label, property));
  }

  /**
   * @brief - Returns vector of keys of label-property indices.
   */
  std::vector<LabelPropertyIndex::Key> GetIndicesKeys() {
    DCHECK(!commited_ && !aborted_) << "Accessor committed or aborted";
    return db_.label_property_index_.Keys();
  }

  /**
   * Return approximate number of all vertices in the database.
   * Note that this is always an over-estimate and never an under-estimate.
   */
  int64_t VerticesCount() const;

  /*
   * Return approximate number of all edges in the database.
   * Note that this is always an over-estimate and never an under-estimate.
   */
  int64_t EdgesCount() const;

  /**
   * Return approximate number of vertices under indexes with the given label.
   * Note that this is always an over-estimate and never an under-estimate.
   *
   * @param label - label to check for
   * @return number of vertices with the given label
   */
  int64_t VerticesCount(const GraphDbTypes::Label &label) const;

  /**
   * Return approximate number of vertices under indexes with the given label
   * and property.  Note that this is always an over-estimate and never an
   * under-estimate.
   *
   * @param label - label to check for
   * @param property - property to check for
   * @return number of vertices with the given label, fails if no such
   * label+property index exists.
   */
  int64_t VerticesCount(const GraphDbTypes::Label &label,
                        const GraphDbTypes::Property &property) const;

  /**
   * Returns approximate number of vertices that have the given label
   * and the given value for the given property.
   *
   * Assumes that an index for that (label, property) exists.
   */
  int64_t VerticesCount(const GraphDbTypes::Label &label,
                        const GraphDbTypes::Property &property,
                        const PropertyValue &value) const;

  /**
   * Returns approximate number of vertices that have the given label
   * and whose vaue is in the range defined by upper and lower @c Bound.
   *
   * At least one bound must be specified. Neither can be
   * PropertyValue::Null.
   *
   * Assumes that an index for that (label, property) exists.
   */
  int64_t VerticesCount(
      const GraphDbTypes::Label &label, const GraphDbTypes::Property &property,
      const std::experimental::optional<utils::Bound<PropertyValue>> lower,
      const std::experimental::optional<utils::Bound<PropertyValue>> upper)
      const;

  /**
   * Obtains the Label for the label's name.
   * @return  See above.
   */
  GraphDbTypes::Label Label(const std::string &label_name);

  /**
   * Obtains the label name (a string) for the given label.
   *
   * @param label a Label.
   * @return  See above.
   */
  const std::string &LabelName(const GraphDbTypes::Label label) const;

  /**
   * Obtains the EdgeType for it's name.
   * @return  See above.
   */
  GraphDbTypes::EdgeType EdgeType(const std::string &edge_type_name);

  /**
   * Obtains the edge type name (a string) for the given edge type.
   *
   * @param edge_type an EdgeType.
   * @return  See above.
   */
  const std::string &EdgeTypeName(const GraphDbTypes::EdgeType edge_type) const;

  /**
   * Obtains the Property for it's name.
   * @return  See above.
   */
  GraphDbTypes::Property Property(const std::string &property_name);

  /**
   * Obtains the property name (a string) for the given property.
   *
   * @param property a Property.
   * @return  See above.
   */
  const std::string &PropertyName(const GraphDbTypes::Property property) const;

  /** Returns the id of this accessor's transaction */
  tx::transaction_id_t transaction_id() const;

  /** Advances transaction's command id by 1. */
  void AdvanceCommand();

  /** Commit transaction. */
  void Commit();

  /** Abort transaction. */
  void Abort();

  /** Return true if transaction is hinted to abort. */
  bool should_abort() const;

  /** Returns the transaction of this accessor */
  const tx::Transaction &transaction() const { return *transaction_; }

  /** Return's the database's write-ahead log */
  durability::WriteAheadLog &wal();

  /**
   * Returns the current value of the counter with the given name, and
   * increments that counter. If the counter with the given name does not exist,
   * a new counter is created and this function returns 0.
   */
  int64_t Counter(const std::string &name);

  /**
   * Sets the counter with the given name to the given value. Returns nothing.
   * If the counter with the given name does not exist, a new counter is created
   * and set to the given value.
   */
  void CounterSet(const std::string &name, int64_t value);

  /*
   * Returns a list of index names present in the database.
   */
  std::vector<std::string> IndexInfo() const;

 private:
  /**
   * Insert this vertex into corresponding label and label+property (if it
   * exists) index.
   *
   * @param label - label with which to insert vertex label record
   * @param vertex_accessor - vertex_accessor to insert
   * @param vertex - vertex record to insert
   */
  void UpdateLabelIndices(const GraphDbTypes::Label &label,
                          const VertexAccessor &vertex_accessor,
                          const Vertex *const vertex);

  /**
   * Insert this vertex into corresponding any label + 'property' index.
   * @param property - vertex will be inserted into indexes which contain this
   * property
   * @param record_accessor - record_accessor to insert
   * @param vertex - vertex to insert
   */
  void UpdatePropertyIndex(const GraphDbTypes::Property &property,
                           const RecordAccessor<Vertex> &record_accessor,
                           const Vertex *const vertex);

  /** Returns true if the given accessor (made with this GraphDbAccessor) is
   * visible given the `current_state` flag. */
  template <typename TRecord>
  bool Visible(const RecordAccessor<TRecord> &accessor,
               bool current_state) const {
    return (accessor.old_ &&
            !(current_state && accessor.old_->is_expired_by(*transaction_))) ||
           (current_state && accessor.new_ &&
            !accessor.new_->is_expired_by(*transaction_));
  }

  /** Casts the DB's engine to MasterEngine and returns it. If the DB's engine
   * is
   * RemoteEngine, this function will crash MG. */
  tx::MasterEngine &MasterEngine() {
    auto *local_engine = dynamic_cast<tx::MasterEngine *>(db_.tx_engine_.get());
    DCHECK(local_engine) << "Asked for MasterEngine on distributed worker";
    return *local_engine;
  }

  GraphDb &db_;

  /** The current transaction */
  tx::Transaction *const transaction_;

  bool commited_{false};
  bool aborted_{false};
};
