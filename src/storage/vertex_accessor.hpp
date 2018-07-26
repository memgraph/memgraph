#pragma once

#include <limits>
#include <set>
#include <vector>

#include "cppitertools/chain.hpp"
#include "cppitertools/imap.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/record_accessor.hpp"
#include "storage/vertex.hpp"
#include "utils/algorithm.hpp"

/**
 * Provides ways for the client programmer (i.e. code generated
 * by the compiler) to interact with a Vertex.
 *
 * This class indirectly inherits MVCC data structures and
 * takes care of MVCC versioning.
 */
class VertexAccessor final : public RecordAccessor<Vertex> {
  using VertexAddress = storage::Address<mvcc::VersionList<Vertex>>;
  // Helper function for creating an iterator over edges.
  // @param begin - begin iterator
  // @param end - end iterator
  // @param from - if true specifies that the vertex represents `from` part of
  // the edge, otherwise it specifies `to` part of the edge
  // @param vertex - one endpoint of every edge
  // @param db_accessor - database accessor
  // @return - Iterator over EdgeAccessors
  template <typename TIterator>
  static inline auto MakeAccessorIterator(
      TIterator &&begin, TIterator &&end, bool from, VertexAddress vertex,
      database::GraphDbAccessor &db_accessor) {
    return iter::imap(
        [from, vertex, &db_accessor](auto &edges_element) {
          if (from) {
            return EdgeAccessor(edges_element.edge, db_accessor, vertex,
                                edges_element.vertex, edges_element.edge_type);
          } else {
            return EdgeAccessor(edges_element.edge, db_accessor,
                                edges_element.vertex, vertex,
                                edges_element.edge_type);
          }
        },
        utils::Iterable<TIterator>(std::forward<TIterator>(begin),
                                   std::forward<TIterator>(end)));
  }

 public:
  /** Like RecordAccessor::Impl with addition of Vertex specific methods. */
  class Impl : public RecordAccessor<Vertex>::Impl {
   public:
    virtual void AddLabel(const VertexAccessor &va,
                          const storage::Label &label) = 0;
    virtual void RemoveLabel(const VertexAccessor &va,
                             const storage::Label &label) = 0;
  };

  VertexAccessor(VertexAddress address, database::GraphDbAccessor &db_accessor);

  /** Returns the number of outgoing edges. */
  size_t out_degree() const;

  /** Returns the number of incoming edges. */
  size_t in_degree() const;

  /** Adds a label to the Vertex. If the Vertex already has that label the call
   * has no effect. */
  void add_label(storage::Label label);

  /** Removes a label from the Vertex. */
  void remove_label(storage::Label label);

  /** Indicates if the Vertex has the given label. */
  bool has_label(storage::Label label) const;

  /** Returns all the Labels of the Vertex. */
  const std::vector<storage::Label> &labels() const;

  /** Returns EdgeAccessors for all incoming edges. */
  auto in() const {
    return MakeAccessorIterator(current().in_.begin(), current().in_.end(),
                                false, address(), db_accessor());
  }

  /**
   * Returns EdgeAccessors for all incoming edges.
   *
   * @param dest - The destination vertex filter.
   * @param edge_types - Edge types filter. At least one be matched. If nullptr
   * or empty, the parameter is ignored.
   */
  auto in(const VertexAccessor &dest,
          const std::vector<storage::EdgeType> *edge_types = nullptr) const {
    return MakeAccessorIterator(current().in_.begin(dest.address(), edge_types),
                                current().in_.end(), false, address(),
                                db_accessor());
  }

  /**
   * Returns EdgeAccessors for all incoming edges.
   *
   * @param edge_types - Edge types filter. At least one be matched. If nullptr
   * or empty, the parameter is ignored.
   */
  auto in(const std::vector<storage::EdgeType> *edge_types) const {
    return MakeAccessorIterator(
        current().in_.begin(storage::VertexAddress(nullptr), edge_types),
        current().in_.end(), false, address(), db_accessor());
  }

  /** Returns EdgeAccessors for all outgoing edges. */
  auto out() const {
    return MakeAccessorIterator(current().out_.begin(), current().out_.end(),
                                true, address(), db_accessor());
  }

  /**
   * Returns EdgeAccessors for all outgoing edges whose destination is the given
   * vertex.
   *
   * @param dest - The destination vertex filter.
   * @param edge_types - Edge types filter. At least one be matched. If nullptr
   * or empty, the parameter is ignored.
   */
  auto out(const VertexAccessor &dest,
           const std::vector<storage::EdgeType> *edge_types = nullptr) const {
    return MakeAccessorIterator(
        current().out_.begin(dest.address(), edge_types), current().out_.end(),
        true, address(), db_accessor());
  }

  /**
   * Returns EdgeAccessors for all outgoing edges.
   *
   * @param edge_types - Edge types filter. At least one be matched. If nullptr
   * or empty, the parameter is ignored.
   */
  auto out(const std::vector<storage::EdgeType> *edge_types) const {
    return MakeAccessorIterator(
        current().out_.begin(storage::VertexAddress(nullptr), edge_types),
        current().out_.end(), true, address(), db_accessor());
  }

  /** Removes the given edge from the outgoing edges of this vertex. Note that
   * this operation should always be accompanied by the removal of the edge from
   * the incoming edges on the other side and edge deletion. */
  void RemoveOutEdge(storage::EdgeAddress edge);

  /** Removes the given edge from the incoming edges of this vertex. Note that
   * this operation should always be accompanied by the removal of the edge from
   * the outgoing edges on the other side and edge deletion. */
  void RemoveInEdge(storage::EdgeAddress edge);

 private:
  Impl *impl_{nullptr};
};

std::ostream &operator<<(std::ostream &, const VertexAccessor &);

// hash function for the vertex accessor
namespace std {
template <>
struct hash<VertexAccessor> {
  size_t operator()(const VertexAccessor &v) const { return v.gid(); };
};
}  // namespace std
