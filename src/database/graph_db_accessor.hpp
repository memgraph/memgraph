//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 03.02.17.
//

#pragma once

#include "cppitertools/filter.hpp"
#include "cppitertools/imap.hpp"

#include "graph_db.hpp"
#include "transactions/transaction.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

/**
 * An accessor for the database object: exposes functions
 * for operating on the database. All the functions in
 * this class should be self-sufficient: for example the
 * function for creating
 * a new Vertex should take care of all the book-keeping around
 * the creation.
 */

class GraphDbAccessor {
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
   */
  auto vertices() {
    // wrap version lists into accessors, which will look for visible versions
    auto accessors =
        iter::imap([this](auto vlist) { return VertexAccessor(*vlist, *this); },
                   db_.vertices_.access());

    // filter out the accessors not visible to the current transaction
    return iter::filter(
        [this](const VertexAccessor &accessor) {
          return accessor.old_ != nullptr;
        },
        std::move(accessors));
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
   */
  auto edges() {
    // wrap version lists into accessors, which will look for visible versions
    auto accessors =
        iter::imap([this](auto vlist) { return EdgeAccessor(*vlist, *this); },
                   db_.edges_.access());

    // filter out the accessors not visible to the current transaction
    return iter::filter(
        [this](const EdgeAccessor &accessor) {
          return accessor.old_ != nullptr;
        },
        std::move(accessors));
  }

  /**
   * Insert this record into corresponding label index.
   * @param label - label index into which to insert record
   * @param record - record which to insert
   */
  template <typename TRecord>
  void update_index(const GraphDbTypes::Label &label, const TRecord &record) {
    db_.labels_index_.Add(label, record.vlist_);
  }

  /**
   * Return VertexAccessors which contain the current label for the current
   * transaction visibilty.
   * @param label - label for which to return VertexAccessors
   * @return iterable collection
   */
  auto vertices_by_label(const GraphDbTypes::Label &label) {
    return iter::imap(
        [this](auto vlist) { return VertexAccessor(*vlist, *this); },
        db_.labels_index_.Acquire(label, *transaction_));
  }
  /**
   * Return approximate number of vertices under indexes with the given label.
   * Note that this is always an over-estimate and never an under-estimate.
   * @param label - label to check for
   * @return number of vertices with the given label
   */
  size_t vertices_by_label_count(const GraphDbTypes::Label &label) {
    return db_.labels_index_.Count(label);
  }

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
  std::string &label_name(const GraphDbTypes::Label label) const;

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
  std::string &edge_type_name(const GraphDbTypes::EdgeType edge_type) const;

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
  std::string &property_name(const GraphDbTypes::Property property) const;

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
   */
  template <typename TRecord>
  void Reconstruct(RecordAccessor<TRecord> &accessor) {
    accessor.vlist_->find_set_new_old(*transaction_, accessor.old_,
                                      accessor.new_);
    accessor.current_ = accessor.old_ ? accessor.old_ : accessor.new_;
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
  GraphDb &db_;

  /** The current transaction */
  tx::Transaction *const transaction_;

  bool commited_{false};
  bool aborted_{false};
};
