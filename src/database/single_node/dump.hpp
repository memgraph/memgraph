#pragma once

#include <ostream>

#include "database/graph_db_accessor.hpp"

namespace database {

/// Class which generates sequence of openCypher queries which can be used to
/// dump the database state.
///
/// Currently only dumps index keys, vertices and edges, one-by-one in multiple
/// queries.
// TODO(tsabolcec): We should also dump constraints once that functionality is
// integrated in MemGraph.
class CypherDumpGenerator {
 public:
  explicit CypherDumpGenerator(GraphDbAccessor *dba);

  CypherDumpGenerator(const CypherDumpGenerator &other) = delete;
  // NOLINTNEXTLINE(performance-noexcept-move-constructor)
  CypherDumpGenerator(CypherDumpGenerator &&other) = default;
  CypherDumpGenerator &operator=(const CypherDumpGenerator &other) = delete;
  CypherDumpGenerator &operator=(CypherDumpGenerator &&other) = delete;
  ~CypherDumpGenerator() = default;

  bool NextQuery(std::ostream *os);

 private:
  // A helper class that keeps container and its iterators.
  template <typename TContainer>
  class ContainerState {
   public:
    explicit ContainerState(TContainer container)
        : container_(std::move(container)),
          current_(container_.begin()),
          end_(container_.end()),
          empty_(current_ == end_) {}

    ContainerState(const ContainerState &other) = delete;
    // NOLINTNEXTLINE(hicpp-noexcept-move,performance-noexcept-move-constructor)
    ContainerState(ContainerState &&other) = default;
    ContainerState &operator=(const ContainerState &other) = delete;
    ContainerState &operator=(ContainerState &&other) = delete;
    ~ContainerState() = default;

    auto GetCurrentAndAdvance() {
      auto to_be_returned = current_;
      if (current_ != end_) ++current_;
      return to_be_returned;
    }

    bool ReachedEnd() const { return current_ == end_; }

    // Returns true iff the container is empty.
    bool Empty() const { return empty_; }

   private:
    TContainer container_;

    using TIterator = decltype(container_.begin());
    TIterator current_;
    TIterator end_;

    bool empty_;
  };

  GraphDbAccessor *dba_;

  bool created_internal_index_;
  bool cleaned_internal_index_;
  bool cleaned_internal_label_property_;

  std::optional<ContainerState<std::vector<LabelPropertyIndex::Key>>>
      indices_state_;
  std::optional<ContainerState<decltype(dba_->Vertices(false))>>
      vertices_state_;
  std::optional<ContainerState<decltype(dba_->Edges(false))>> edges_state_;
};

}  // namespace database
