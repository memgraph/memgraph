//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 03.02.17.
//

#pragma

#include "graph_db.hpp"
#include "transactions/transaction.hpp"


class GraphDbAccessor {
  GraphDbAccessor(GraphDb& db) : db_(db), transaction_(db.tx_engine.begin()) {}

public:
  /**
   * Creates a new Vertex and returns an accessor to it.
   *
   * @return See above.
   */
  VertexAccessor insert_vertex();

  /**
   * Creates a new Edge and returns an accessor to it.
   *
   * @param from The 'from' vertex.
   * @param to The 'to' vertex'
   * @param type Edge type.
   * @return  An accessor to the edge.
   */
  EdgeAccessor insert_edge(VertexAccessor& from, VertexAccessor& to, GraphDb::EdgeType type);

  /**
   * Obtains the Label for the label's name.
   * @return  See above.
   */
  GraphDb::Label label(const std::string& label_name);

  /**
   * Obtains the EdgeType for it's name.
   * @return  See above.
   */
  GraphDb::EdgeType edge_type(const std::string& edge_type_name);

  /**
   * Obtains the Property for it's name.
   * @return  See above.
   */
  GraphDb::Property property(const std::string& property_name);

  /** The current transaction */
  tx::Transaction const transaction_;

private:
  GraphDb& db_;

  // for privileged access to some RecordAccessor functionality (and similar)
  const PassKey<GraphDb> pass_key;
};
