/// @file

#pragma once

#include <optional>

#include "storage/distributed/edge_accessor.hpp"
#include "storage/distributed/edges.hpp"
#include "storage/distributed/vertex.hpp"

class VertexAccessor;
/// EdgeAccessorIterator is a forward iterator that returns EdgeAccessor for
/// every edge in a Vertex. It also makes sure to keep the data it iterates over
/// alive, this is important because of the LRU cache that evicts data when its
/// full.
class EdgeAccessorIterator {
 public:
  EdgeAccessorIterator(Edges::Iterator iter,
                       const std::shared_ptr<VertexAccessor> &va, bool from)
      : va_(va), iter_(iter), from_(from) {}

  EdgeAccessorIterator(const EdgeAccessorIterator &other)
      : va_(other.va_), iter_(other.iter_), from_(other.from_) {}

  EdgeAccessorIterator &operator=(const EdgeAccessorIterator &other) {
    va_ = other.va_;
    iter_ = other.iter_;
    from_ = other.from_;
    return *this;
  }

  EdgeAccessorIterator &operator++() {
    ++iter_;
    ResetAccessor();
    return *this;
  }

  EdgeAccessorIterator operator++(int) {
    auto old = *this;
    ++iter_;
    ResetAccessor();
    return old;
  }

  EdgeAccessor &operator*() {
    UpdateAccessor();
    return *edge_accessor_;
  }

  EdgeAccessor *operator->() {
    UpdateAccessor();
    return &edge_accessor_.value();
  }

  bool operator==(const EdgeAccessorIterator &other) const {
    return iter_ == other.iter_;
  }

  bool operator!=(const EdgeAccessorIterator &other) const {
    return !(*this == other);
  }

 private:
  void UpdateAccessor() {
    if (edge_accessor_) return;

    if (from_) {
      CreateOut(*iter_);
    } else {
      CreateIn(*iter_);
    }
  }

  void ResetAccessor() { edge_accessor_ = std::nullopt; }

  void CreateOut(const Edges::Element &e);

  void CreateIn(const Edges::Element &e);

  std::shared_ptr<VertexAccessor> va_;
  std::optional<EdgeAccessor> edge_accessor_;
  Edges::Iterator iter_;
  bool from_;
};

/// EdgesIterable constains begin and end iterator to edges stored in vertex.
/// It has constructors that can be used to create iterators that skip over
/// some elements.
class EdgesIterable {
 public:
  /// Creates new iterable that will skip edges whose destination vertex is not
  /// equal to dest and whose type is not in edge_types.
  ///
  /// @param dest - the destination vertex address, if empty the edges will not
  /// be filtered on destination
  /// @param edge_types - the edge types at least one of which must be matched,
  /// if nullptr edges are not filtered on type
  EdgesIterable(const VertexAccessor &va, bool from, const VertexAccessor &dest,
                const std::vector<storage::EdgeType> *edge_types = nullptr);

  /// Creates new iterable that will skip edges whose type is not in edge_types.
  ///
  /// @param edge_types - the edge types at least one of which must be matched,
  /// if nullptr edges are not filtered on type
  EdgesIterable(const VertexAccessor &va, bool from,
                const std::vector<storage::EdgeType> *edge_types = nullptr);

  EdgeAccessorIterator begin() { return *begin_; };

  EdgeAccessorIterator end() { return *end_; };

 private:
  EdgeAccessorIterator GetBegin(
      std::shared_ptr<VertexAccessor> va, bool from,
      std::optional<storage::VertexAddress> dest,
      const std::vector<storage::EdgeType> *edge_types = nullptr);

  EdgeAccessorIterator GetEnd(std::shared_ptr<VertexAccessor> va, bool from);

  std::optional<EdgeAccessorIterator> begin_;
  std::optional<EdgeAccessorIterator> end_;
};
