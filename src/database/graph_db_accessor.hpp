//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 03.02.17.
//

#pragma once

#include <experimental/optional>

#include "cppitertools/filter.hpp"
#include "cppitertools/imap.hpp"

#include "graph_db.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "transactions/transaction.hpp"
#include "utils/bound.hpp"

/** Thrown when creating an index which already exists. */
class IndexExistsException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

/**
 * An accessor for the database object: exposes functions
 * for operating on the database. All the functions in
 * this class should be self-sufficient: for example the
 * function for creating
 * a new Vertex should take care of all the book-keeping around
 * the creation.
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
  GraphDbAccessor(GraphDb &db);
  ~GraphDbAccessor();

  // the GraphDbAccessor can NOT be copied nor moved because
  // 1. it ensures transaction cleanup once it's destructed
  // 2. it will contain index and side-effect bookkeeping data
  //    which is unique to the transaction (shared_ptr works but slower)
  GraphDbAccessor(const GraphDbAccessor &other) = delete;
  GraphDbAccessor(GraphDbAccessor &&other) = delete;
  GraphDbAccessor &operator=(const GraphDbAccessor &other) = delete;
  GraphDbAccessor &operator=(GraphDbAccessor &&other) = delete;

  /**
   * Returns the name of the database of this accessor.
   */
  const std::string &name() const;

  /**
   * Creates a new Vertex and returns an accessor to it.
   *
   * @return See above.
   */
  VertexAccessor insert_vertex();

  /**
   * Removes the vertex of the given accessor. If the vertex has any outgoing
   * or incoming edges, it is not deleted. See `detach_remove_vertex` if you
   * want to remove a vertex regardless of connectivity.
   *
   * If the vertex has already been deleted by the current transaction+command,
   * this function will not do anything and will return true.
   *
   * @param vertex_accessor Accessor to vertex.
   * @return  If or not the vertex was deleted.
   */
  bool remove_vertex(VertexAccessor &vertex_accessor);

  /**
   * Removes the vertex of the given accessor along with all it's outgoing
   * and incoming connections.
   *
   * @param vertex_accessor  Accessor to a vertex.
   */
  void detach_remove_vertex(VertexAccessor &vertex_accessor);

  /**
   * Returns iterable over accessors to all the vertices in the graph
   * visible to the current transaction.
   *
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   */
  auto vertices(bool current_state) {
    // wrap version lists into accessors, which will look for visible versions
    auto accessors =
        iter::imap([this](auto vlist) { return VertexAccessor(*vlist, *this); },
                   db_.vertices_.access());

    // filter out the accessors not visible to the current transaction
    return iter::filter(
        [this, current_state](const VertexAccessor &accessor) {
          return (accessor.old_ &&
                  !(current_state &&
                    accessor.old_->is_deleted_by(*transaction_))) ||
                 (current_state && accessor.new_ &&
                  !accessor.new_->is_deleted_by(*transaction_));
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
  auto vertices(const GraphDbTypes::Label &label, bool current_state) {
    return iter::imap(
        [this, current_state](auto vlist) {
          return VertexAccessor(*vlist, *this);
        },
        db_.labels_index_.GetVlists(label, *transaction_, current_state));
  }

  /**
   * Return VertexAccessors which contain the current label and property for the
   * given transaction visibility.
   * @param label - label for which to return VertexAccessors
   * @param property - property for which to return VertexAccessors
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   * @return iterable collection
   */
  auto vertices(const GraphDbTypes::Label &label,
                const GraphDbTypes::Property &property, bool current_state) {
    debug_assert(db_.label_property_index_.IndexExists(
                     LabelPropertyIndex::Key(label, property)),
                 "Label+property index doesn't exist.");
    return iter::imap([this, current_state](
                          auto vlist) { return VertexAccessor(*vlist, *this); },
                      db_.label_property_index_.GetVlists(
                          LabelPropertyIndex::Key(label, property),
                          *transaction_, current_state));
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
  auto vertices(const GraphDbTypes::Label &label,
                const GraphDbTypes::Property &property,
                const PropertyValue &value, bool current_state) {
    debug_assert(db_.label_property_index_.IndexExists(
                     LabelPropertyIndex::Key(label, property)),
                 "Label+property index doesn't exist.");
    debug_assert(value.type() != PropertyValue::Type::Null,
                 "Can't query index for propery value type null.");
    return iter::imap([this, current_state](
                          auto vlist) { return VertexAccessor(*vlist, *this); },
                      db_.label_property_index_.GetVlists(
                          LabelPropertyIndex::Key(label, property), value,
                          *transaction_, current_state));
  }

  /**
   * Creates a new Edge and returns an accessor to it.
   *
   * @param from The 'from' vertex.
   * @param to The 'to' vertex'
   * @param type Edge type.
   * @return  An accessor to the edge.
   */
  EdgeAccessor insert_edge(VertexAccessor &from, VertexAccessor &to,
                           GraphDbTypes::EdgeType type);

  /**
   * Removes an edge from the graph.
   *
   * @param edge_accessor  The accessor to an edge.
   */
  void remove_edge(EdgeAccessor &edge_accessor);

  /**
   * Returns iterable over accessors to all the edges in the graph
   * visible to the current transaction.
   *
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   */
  auto edges(bool current_state) {
    // wrap version lists into accessors, which will look for visible versions
    auto accessors =
        iter::imap([this](auto vlist) { return EdgeAccessor(*vlist, *this); },
                   db_.edges_.access());

    // filter out the accessors not visible to the current transaction
    return iter::filter(
        [this, current_state](const EdgeAccessor &accessor) {
          return (accessor.old_ &&
                  !(current_state &&
                    accessor.old_->is_deleted_by(*transaction_))) ||
                 (current_state && accessor.new_ &&
                  !accessor.new_->is_deleted_by(*transaction_));
        },
        std::move(accessors));
  }

  /**
   * Return EdgeAccessors which contain the edge_type for the current
   * transaction visibility.
   * @param edge_type - edge_type for which to return EdgeAccessors
   * @param current_state If true then the graph state for the
   *    current transaction+command is returned (insertions, updates and
   *    deletions performed in the current transaction+command are not
   *    ignored).
   * @return iterable collection
   */
  auto edges(const GraphDbTypes::EdgeType &edge_type, bool current_state) {
    return iter::imap([this, current_state](
                          auto vlist) { return EdgeAccessor(*vlist, *this); },
                      db_.edge_types_index_.GetVlists(edge_type, *transaction_,
                                                      current_state));
  }

  /**
   * @brief - Build the given label/property Index. BuildIndex shouldn't be
   * called in the same transaction where you are creating/or modifying
   * vertices/edges. It should be a part of a separate transaction. Blocks the
   * caller until the index is ready for use. Throws exception if index already
   * exists or is being created by another transaction.
   * @param label - label to build for
   * @param property - property to build for
   */
  void BuildIndex(const GraphDbTypes::Label &label,
                  const GraphDbTypes::Property &property) {
    const LabelPropertyIndex::Key key(label, property);
    if (db_.label_property_index_.CreateIndex(key) == false) {
      throw IndexExistsException(
          "Index is either being created by another transaction or already "
          "exists.");
    }
    // Everything that happens after the line above ended will be added to the
    // index automatically, but we still have to add to index everything that
    // happened earlier. We have to first wait for every transaction that
    // happend before, or a bit later than CreateIndex to end.
    {
      auto wait_transaction = db_.tx_engine.Begin();
      for (auto id : wait_transaction->snapshot()) {
        if (id == transaction_->id_) continue;
        while (wait_transaction->engine_.clog().fetch_info(id).is_active())
          // TODO reconsider this constant, currently rule-of-thumb chosen
          std::this_thread::sleep_for(std::chrono::microseconds(100));
      }
      wait_transaction->Commit();
    }

    // This transaction surely sees everything that happened before CreateIndex.
    auto transaction = db_.tx_engine.Begin();

    for (auto vertex_vlist : db_.vertices_.access()) {
      auto vertex_record = vertex_vlist->find(*transaction);
      // Check if visible record exists, if it exists apply function on it.
      if (vertex_record == nullptr) continue;
      db_.label_property_index_.UpdateOnLabelProperty(vertex_vlist,
                                                      vertex_record);
    }
    // Commit transaction as we finished applying method on newest visible
    // records.
    transaction->Commit();
    // After these two operations we are certain that everything is contained in
    // the index under the assumption that this transaction contained no
    // vertex/edge insert/update before this method was invoked.
    db_.label_property_index_.IndexFinishedBuilding(key);
  }

  /**
   * @brief - Returns true if the given label+property index already exists and
   * is ready for use.
   */
  bool LabelPropertyIndexExists(const GraphDbTypes::Label &label,
                                const GraphDbTypes::Property &property) const {
    return db_.label_property_index_.IndexExists(
        LabelPropertyIndex::Key(label, property));
  }

  /**
   * Return approximate number of all vertices in the database.
   * Note that this is always an over-estimate and never an under-estimate.
   */
  int64_t vertices_count() const;

  /*
   * Return approximate number of all edges in the database.
   * Note that this is always an over-estimate and never an under-estimate.
   */
  int64_t edges_count() const;

  /**
   * Return approximate number of vertices under indexes with the given label.
   * Note that this is always an over-estimate and never an under-estimate.
   * @param label - label to check for
   * @return number of vertices with the given label
   */
  int64_t vertices_count(const GraphDbTypes::Label &label) const;

  /**
   * Return approximate number of vertices under indexes with the given label
   * and property.
   * Note that this is always an over-estimate and never an under-estimate.
   * @param label - label to check for
   * @param property - property to check for
   * @return number of vertices with the given label, fails if no such
   * label+property index exists.
   */
  int64_t vertices_count(const GraphDbTypes::Label &label,
                         const GraphDbTypes::Property &property) const;

  /**
   * Returns approximate number of vertices that have the given label
   * and the given value for the given property.
   *
   * Assumes that an index for that (label, property) exists.
   */
  int64_t vertices_count(const GraphDbTypes::Label &label,
                         const GraphDbTypes::Property &property,
                         const PropertyValue &value) const;

  /**
   * Returns approximate number of vertices that have the given label
   * and whose vaue is in the range defined by upper and lower @c Bound.
   * At least one bound must be specified. If lower bound is not specified,
   * the whole upper bound prefix is returned.
   *
   * Assumes that an index for that (label, property) exists.
   */
  int64_t vertices_count(
      const GraphDbTypes::Label &label, const GraphDbTypes::Property &property,
      const std::experimental::optional<utils::Bound<PropertyValue>> lower,
      const std::experimental::optional<utils::Bound<PropertyValue>> upper)
      const;

  /**
   * Return approximate number of edges under indexes with the given edge_type.
   * Note that this is always an over-estimate and never an under-estimate.
   * @param edge_type - edge_type to check for
   * @return number of edges with the given edge_type
   */
  int64_t edges_count(const GraphDbTypes::EdgeType &edge_type) const;

  /**
   * Obtains the Label for the label's name.
   * @return  See above.
   */
  GraphDbTypes::Label label(const std::string &label_name);

  /**
   * Obtains the label name (a string) for the given label.
   *
   * @param label a Label.
   * @return  See above.
   */
  const std::string &label_name(const GraphDbTypes::Label label) const;

  /**
   * Obtains the EdgeType for it's name.
   * @return  See above.
   */
  GraphDbTypes::EdgeType edge_type(const std::string &edge_type_name);

  /**
   * Obtains the edge type name (a string) for the given edge type.
   *
   * @param edge_type an EdgeType.
   * @return  See above.
   */
  const std::string &edge_type_name(
      const GraphDbTypes::EdgeType edge_type) const;

  /**
   * Obtains the Property for it's name.
   * @return  See above.
   */
  GraphDbTypes::Property property(const std::string &property_name);

  /**
   * Obtains the property name (a string) for the given property.
   *
   * @param property a Property.
   * @return  See above.
   */
  const std::string &property_name(const GraphDbTypes::Property property) const;

  /**
   * Advances transaction's command id by 1.
   */
  void advance_command();

  /**
   * Commit transaction.
   */
  void commit();

  /**
   * Abort transaction.
   */
  void abort();

  /**
   * Initializes the record pointers in the given accessor.
   * The old_ and new_ pointers need to be initialized
   * with appropriate values, and current_ set to old_
   * if it exists and to new_ otherwise.
   *
   * @return True if accessor is valid after reconstruction.
   * This means that at least one record pointer was found
   * (either new_ or old_), possibly both.
   */
  template <typename TRecord>
  bool Reconstruct(RecordAccessor<TRecord> &accessor) {
    accessor.vlist_->find_set_old_new(*transaction_, accessor.old_,
                                      accessor.new_);
    accessor.current_ = accessor.old_ ? accessor.old_ : accessor.new_;
    return accessor.old_ != nullptr || accessor.new_ != nullptr;
    // TODO review: we should never use a record accessor that
    // does not have either old_ or new_ (both are null), but
    // we can't assert that here because we construct such an accessor
    // and filter it out in GraphDbAccessor::[Vertices|Edges]
    // any ideas?
  }

  /**
   * Update accessor record with vlist.
   *
   * It is not legal
   * to call this function on a Vertex/Edge that has
   * been deleted in the current transaction+command.
   *
   * @args accessor whose record to update if possible.
   */
  template <typename TRecord>
  void update(RecordAccessor<TRecord> &accessor) {
    // can't update a deleted record if:
    // - we only have old_ and it hasn't been deleted
    // - we have new_ and it hasn't been deleted
    if (!accessor.new_) {
      debug_assert(
          !accessor.old_->is_deleted_by(*transaction_),
          "Can't update a record deleted in the current transaction+command");
    } else {
      debug_assert(
          !accessor.new_->is_deleted_by(*transaction_),
          "Can't update a record deleted in the current transaction+command");
    }

    if (!accessor.new_) accessor.new_ = accessor.vlist_->update(*transaction_);
  }

 private:
  /**
   * Insert this vertex into corresponding label and label+property (if it
   * exists) index.
   * @param label - label with which to insert vertex label record
   * @param vertex_accessor - vertex_accessor to insert
   * @param vertex - vertex record to insert
   */
  void update_label_indices(const GraphDbTypes::Label &label,
                            const VertexAccessor &vertex_accessor,
                            const Vertex *const vertex);

  /**
   * Insert this edge into corresponding edge_type index.
   * @param edge_type  - edge_type index into which to insert record
   * @param edge_accessor - edge_accessor to insert
   * @param edge - edge record to insert
   */
  void update_edge_type_index(const GraphDbTypes::EdgeType &edge_type,
                              const EdgeAccessor &edge_accessor,
                              const Edge *const edge);

  /**
   * Insert this vertex into corresponding any label + 'property' index.
   * @param property - vertex will be inserted into indexes which contain this
   * property
   * @param record_accessor - record_accessor to insert
   * @param vertex - vertex to insert
   */
  void update_property_index(const GraphDbTypes::Property &property,
                             const RecordAccessor<Vertex> &record_accessor,
                             const Vertex *const vertex);
  GraphDb &db_;

  /** The current transaction */
  tx::Transaction *const transaction_;

  bool commited_{false};
  bool aborted_{false};
};
