#pragma once

#include <ostream>

#include "database/graph_db_accessor.hpp"

namespace database {

/// Class which generates parts of openCypher query which can be used to dump
/// the database state.
///
/// Currently, all parts combined form a single query which dumps an entire
/// graph (vertices and edges). Since query can be quite long for larger graphs,
/// the graph should be split in multiple queries in the future. Indexes,
/// constraints, roles, etc. are currently not dumped.
class DumpGenerator {
 public:
  explicit DumpGenerator(GraphDbAccessor *dba);

  bool NextQuery(std::ostream *os);

 private:
  // A helper class that keeps container and its iterators.
  template <typename TContainer>
  class ContainerState {
   public:
    explicit ContainerState(TContainer container)
        : container_(std::move(container)),
          current_(container_.begin()),
          end_(container_.end()) {}

    auto GetCurrentAndAdvance() {
      auto to_be_returned = current_;
      if (current_ != end_) ++current_;
      return to_be_returned;
    }

    bool ReachedEnd() const { return current_ == end_; }

   private:
    TContainer container_;

    using TIterator = decltype(container_.begin());
    TIterator current_;
    TIterator end_;
  };

  GraphDbAccessor *dba_;

  // Boolean which indicates if the `NextQuery` method is called for the first
  // time.
  bool first_;

  std::optional<ContainerState<decltype(dba_->Vertices(false))>>
      vertices_state_;
  std::optional<ContainerState<decltype(dba_->Edges(false))>> edges_state_;
};

/// Dumps database state to output stream as openCypher queries.
///
/// Currently, it only dumps vertices and edges of the graph. In the future,
/// it should also dump indexes, constraints, roles, etc.
void DumpToCypher(std::ostream *os, GraphDbAccessor *dba);

}  // namespace database
