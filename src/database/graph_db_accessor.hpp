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
  GraphDbAccessor(GraphDb& db);
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
  const std::string& name() const;

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
  bool remove_vertex(VertexAccessor& vertex_accessor);

  /**
   * Removes the vertex of the given accessor along with all it's outgoing
   * and incoming connections.
   *
   * @param vertex_accessor  Accessor to a vertex.
   */
  void detach_remove_vertex(VertexAccessor& vertex_accessor);

  /**
   * Returns iterable over accessors to all the vertices in the graph
   * visible to the current transaction.
   */
  auto vertices() {
    // filter out the accessors not visible to the current transaction
    auto filtered = iter::filter(
        [this](auto vlist) { return vlist->find(*transaction_) != nullptr; },
        db_.vertices_.access());

    // return accessors of the filtered out vlists
    return iter::imap(
        [this](auto vlist) { return VertexAccessor(*vlist, *this); },
        std::move(filtered));
  }

  /**
   * Creates a new Edge and returns an accessor to it.
   *
   * @param from The 'from' vertex.
   * @param to The 'to' vertex'
   * @param type Edge type.
   * @return  An accessor to the edge.
   */
  EdgeAccessor insert_edge(VertexAccessor& from, VertexAccessor& to,
                           GraphDbTypes::EdgeType type);

  /**
   * Removes an edge from the graph.
   *
   * @param edge_accessor  The accessor to an edge.
   */
  void remove_edge(EdgeAccessor& edge_accessor);

  /**
   * Returns iterable over accessors to all the edges in the graph
   * visible to the current transaction.
   */
  auto edges() {
    // filter out the accessors not visible to the current transaction
    auto filtered = iter::filter(
        [this](auto vlist) { return vlist->find(*transaction_) != nullptr; },
        db_.edges_.access());

    // return accessors of the filtered out vlists
    return iter::imap(
        [this](auto vlist) { return EdgeAccessor(*vlist, *this); },
        std::move(filtered));
  }

  /**
   * Obtains the Label for the label's name.
   * @return  See above.
   */
  GraphDbTypes::Label label(const std::string& label_name);

  /**
   * Obtains the label name (a string) for the given label.
   *
   * @param label a Label.
   * @return  See above.
   */
  std::string& label_name(const GraphDbTypes::Label label) const;

  /**
   * Obtains the EdgeType for it's name.
   * @return  See above.
   */
  GraphDbTypes::EdgeType edge_type(const std::string& edge_type_name);

  /**
   * Obtains the edge type name (a string) for the given edge type.
   *
   * @param edge_type an EdgeType.
   * @return  See above.
   */
  std::string& edge_type_name(const GraphDbTypes::EdgeType edge_type) const;

  /**
   * Obtains the Property for it's name.
   * @return  See above.
   */
  GraphDbTypes::Property property(const std::string& property_name);

  /**
   * Obtains the property name (a string) for the given property.
   *
   * @param property a Property.
   * @return  See above.
   */
  std::string& property_name(const GraphDbTypes::Property property) const;

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
   * Init accessor record with vlist.
   * @args accessor whose record to initialize.
   */
  template <typename TRecord>
  void init_record(RecordAccessor<TRecord>& accessor) {
    accessor.record_ = accessor.vlist_->find(*transaction_);
  }

  /**
   * Update accessor record with vlist.
   * @args accessor whose record to update if possible.
   */
  template <typename TRecord>
  void update(RecordAccessor<TRecord>& accessor) {
    if (!accessor.record_->is_visible_write(*transaction_))
      accessor.record_ = accessor.vlist_->update(*transaction_);
  }

 private:
  GraphDb& db_;

  /** The current transaction */
  tx::Transaction* const transaction_;

  bool commited_{false};
  bool aborted_{false};
};
