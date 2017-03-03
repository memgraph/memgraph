//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 24.01.17..
//

#include <vector>
#include <list>
#include <unordered_set>
#include <iostream>
#include <list>
#include <memory>

#include "gtest/gtest.h"

#include "traversal/enums.hpp"
#include "traversal/path.hpp"
#include "traversal/templates.hpp"

/**
 * A specialization of the "traversal" namespace that uses
 * test Vertex and Edge classes (as opposed to the real database
 * ones).
 */
namespace traversal_test {

// add the standard enums to this namespace
using Direction = traversal_template::Direction;
using Expansion = traversal_template::Expansion;
using Uniqueness = traversal_template::Uniqueness;

// forward declaring Edge because Vertex holds references to it
class Edge;

/**
 * A Vertex class for traversal testing. Has only the
 * traversal capabilities and an exposed id_ member.
 */
class Vertex {
  friend class Edge;

public:
  // unique identifier (uniqueness guaranteed by this class)
  const int id_;

  // vertex set by the user, for testing purposes
  const int label_;

  Vertex(int label=0) : id_(counter_++), label_(label) {}

  const auto &in() const { return *in_; }
  const auto &out() const { return *out_; }

  friend std::ostream &operator<<(std::ostream &stream, const Vertex &v) {
    return stream << "(" << v.id_ << ")";
  }

  bool operator==(const Vertex &v) const { return this->id_ == v.id_; }
  bool operator!=(const Vertex &v) const { return !(*this == v); }

private:
  // shared pointers to outgoing and incoming edges are used so that when
  // a Vertex is copied it still points to the same connectivity storage
  std::shared_ptr<std::vector<Edge>> in_ = std::make_shared<std::vector<Edge>>();
  std::shared_ptr<std::vector<Edge>> out_ = std::make_shared<std::vector<Edge>>();

  // Vertex counter, used for generating IDs
  static int counter_;

};

int Vertex::counter_ = 0;

/**
 * An Edge class for traversal testing. Has only traversal capabilities
 * and an exposed id_ member.
 */
class Edge {
public:
  // unique identifier (uniqueness guaranteed by this class)
  const int id_;

  // vertex set by the user, for testing purposes
  const int type_;

  Edge(const Vertex &from, const Vertex &to, int type=0) :
      id_(counter_++), from_(from), to_(to), type_(type) {
    from.out_->emplace_back(*this);
    to.in_->emplace_back(*this);
  }

  const Vertex &from() const { return from_; }
  const Vertex &to() const { return to_; }

  bool operator==(const Edge &e) const { return id_ == e.id_; }
  bool operator!=(const Edge &e) const { return !(*this == e); }

  friend std::ostream &operator<<(std::ostream &stream, const Edge &e) {
    return stream << "[" << e.id_ << "]";
  }

private:
  const Vertex &from_;
  const Vertex &to_;

  // Edge counter, used for generating IDs
  static int counter_;
};

int Edge::counter_ = 0;

// expose Path and Paths as class template instantiations
using Path = traversal_template::Path<Vertex, Edge>;
using Paths = traversal_template::Paths<Vertex, Edge>;

/**
 * Specialization of the traversal_template::Begin function.
 */
template<typename TCollection>
auto Begin(const TCollection &vertices, std::function<bool(const Vertex &)> vertex_filter = {}) {
  return traversal_template::Begin<TCollection, Vertex, Edge>(vertices, vertex_filter);
}

/**
 * Specialization of the traversal_template::Cartesian function that accepts
 * a single argument.
 */
template<typename TVisitable>
auto Cartesian(TVisitable &&visitable) {
  return traversal_template::Cartesian<TVisitable, Vertex, Edge>(
      std::forward<TVisitable>(visitable));
}


/**
 * Specialization of the traversal_template::Cartesian function that accepts
 * multiple arguments.
 */
template<typename TVisitableFirst, typename... TVisitableOthers>
auto Cartesian(TVisitableFirst &&first, TVisitableOthers &&... others) {
  return traversal_template::CartesianBinaryType<TVisitableFirst, decltype(Cartesian(
      std::forward<TVisitableOthers>(others)...)),
      Vertex, Edge>(
      std::forward<TVisitableFirst>(first),
      Cartesian(std::forward<TVisitableOthers>(others)...)
  );
}
}

/**
 * Hash calculations for text vertex, edge, Path and Paths.
 * Only for testing purposes.
 */
namespace std{

template <>
class hash<traversal_test::Vertex> {
public:
  size_t operator()(const traversal_test::Vertex &vertex) const {
    return (size_t) vertex.id_;
  }
};

template <>
class hash<traversal_test::Edge> {
public:
  size_t operator()(const traversal_test::Edge &edge) const {
    return (size_t) edge.id_;
  }
};

template <>
class hash<traversal_test::Path> {
public:
  std::size_t operator()(const traversal_test::Path &path) const {
    std::size_t r_val = 0;

    for (const auto &vertex : path.Vertices())
      r_val ^= std::hash<traversal_test::Vertex>{}(vertex);

    return r_val;
  }
};

template <>
class hash<traversal_test::Paths> {
public:
  std::size_t operator()(const traversal_test::Paths &paths) const {
    std::size_t r_val = 0;

    for (const auto &path : paths)
      r_val ^= std::hash<traversal_test::Path>{}(path);

    return r_val;
  }
};
}


/**
 * Hash for a list of edges, used in tests.
 */
namespace std{
template <>
class hash<std::list<traversal_test::Edge>> {
public:
  std::size_t operator()(const std::list<traversal_test::Edge> &edges) const {
    std::size_t r_val = 0;

    for (const auto &edge : edges)
      r_val ^= std::hash<traversal_test::Edge>{}(edge);

    return r_val;
  }
};
}

using namespace traversal_test;

template <typename TElement, typename TVisitable>
std::unordered_set<TElement> accumulate_visited(const TVisitable &visitable) {
  // we accept const here so we can receive r-values
  // we cast the const-ness away because the Visit function is not const
  std::unordered_set<TElement> results;
  const_cast<TVisitable&>(visitable).Visit([&results](auto e) { results.insert(e); });
  return results;
};

TEST(Traversal, VertexCreationAndEquality) {
  Vertex v0;
  Vertex v1;
  EXPECT_EQ(v0, v0);
  EXPECT_NE(v0, v1);
}

TEST(Traversal, EdgeCreationAndEquality) {
  Vertex v0;
  Vertex v1;

  Edge e0(v0, v1);
  Edge e1(v0, v1);
  EXPECT_EQ(e0, e0);
  EXPECT_NE(e0, e1);
}

TEST(Traversal, PathCreationAndEquality) {
  Vertex v0;
  Vertex v1;
  Vertex v2;
  Edge e0(v0, v1);
  Edge e1(v0, v2);

  // first test single-vertex-path equality
  Path ref_path;
  ref_path.Start(v0);
  EXPECT_EQ(ref_path, Path().Start(v0));
  EXPECT_NE(ref_path, Path().Start(v1));

  // now test two-vertex-path equality
  // reference path is (v0)-[e0]->(v1)
  ref_path.Append(e0, v1);
  EXPECT_EQ(ref_path, Path().Start(v0).Append(e0, v1));

  // test against one-vertex paths
  EXPECT_NE(ref_path, Path().Start(v0));
  EXPECT_NE(ref_path, Path().Start(v1));

  // test on different vertex
  EXPECT_NE(ref_path, Path().Start(v0).Append(e0, v2));
  // test on different edge
  EXPECT_NE(ref_path, Path().Start(v0).Append(e1, v1));
  // test on different expansion direction
  EXPECT_NE(ref_path, Path().Start(v0).Prepend(e0, v1));
}

TEST(Traversal, Begin) {
  std::vector<Vertex> vertices(3);

  std::unordered_set<Path> expected;
  for (const auto &vertex : vertices)
    expected.insert(Path().Start(vertex));
  EXPECT_EQ(expected.size(), 3);

  auto result = accumulate_visited<Path>(Begin(vertices));
  EXPECT_EQ(result, expected);
}

TEST(Traversal, BeginExpand) {
  Vertex v0, v1, v2, v3;
  Edge e0(v0, v1);
  Edge e1(v0, v2);
  Edge e2(v2, v3);

  // test traversal from (v0)
  auto expected = std::unordered_set<Path>{
      Path().Start(v0).Append(e0, v1),
      Path().Start(v0).Append(e1, v2)
  };
  auto result= accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          Expand(Expansion::Back, Direction::Out));
  EXPECT_EQ(expected, result);
}

TEST(Traversal, BeginExpandVariable) {
  Vertex v0, v1, v2, v3, v4;
  Edge e0(v0, v1);
  Edge e1(v0, v2);
  Edge e2(v2, v3);
  // disconnected from the above graph, just to have something else
  Vertex v5, v6;
  Edge e3(v5, v6);

  // test traversal from (v0)
  auto expected = std::unordered_set<Path>{
      Path().Start(v0).Append(e0, v1),
      Path().Start(v0).Append(e1, v2),
      Path().Start(v0).Append(e1, v2).Append(e2, v3)
  };
  auto result= accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          ExpandVariable(Expansion::Back, Direction::Out));
  EXPECT_EQ(expected, result);

}

TEST(Traversal, BeginFilter) {
  Vertex v0(1), v1(1), v2(2);

  std::unordered_set<Path> expected{
      Path().Start(v0),
      Path().Start(v1)
  };

  auto result = accumulate_visited<Path>(Begin(std::vector<Vertex>{v0, v1, v2},
                                               [](const Vertex &v) { return v.label_ == 1; }));
  EXPECT_EQ(expected, result);

}

TEST(Traversal, BeginCurrent) {
  Vertex v0, v1, v2, v3, v4;

  std::unordered_set<Vertex> expected{v0, v1, v2};
  std::vector<Vertex> begin_input{v0, v1, v2};
  auto b = Begin(begin_input);

  b.Visit([&b, &expected](Path &path){
    EXPECT_EQ(1, expected.erase(b.CurrentVertex()));
  });

  EXPECT_EQ(0, expected.size());
}

TEST(Traversal, ExpandExpand) {
  Vertex v0, v1, v2, v3, v4, v5;
  Edge e0(v0, v1);
  Edge e1(v0, v2);
  Edge e2(v1, v3);
  Edge e3(v2, v4);
  Edge e4(v2, v5);

  // not part of the results
  Vertex v6, v7;
  Edge e5(v4, v6);
  Edge e6(v7, v1);

  // test traversal from (v0)
  auto expected = std::unordered_set<Path>{
      Path().Start(v0).Append(e0, v1).Append(e2, v3),
      Path().Start(v0).Append(e1, v2).Append(e3, v4),
      Path().Start(v0).Append(e1, v2).Append(e4, v5)
  };
  auto result= accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          Expand(Expansion::Back, Direction::Out).
          Expand(Expansion::Back, Direction::Out));
  EXPECT_EQ(expected, result);
}

TEST(Traversal, ExpandFilter) {
  // make a one-level deep tree and 4 children that have {1,2} vertex labels and {3,4} edge types
  Vertex root;
  Vertex v1(1); Edge e1(root, v1, 3);
  Vertex v2(1); Edge e2(root, v2, 4);
  Vertex v3(2); Edge e3(root, v3, 3);
  Vertex v4(2); Edge e4(root, v4, 4);

  // filter to accept only label 1 and edge type 3, expect exactly one path
  auto result = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{root}).
          Expand(Expansion::Back, Direction::Out,
                 [](const Vertex &v) { return v.label_ == 1; },
                 [](const Edge &e) { return e.type_ == 3; }));

  auto expected = std::unordered_set<Path>{Path().Start(root).Append(e1, v1)};
  EXPECT_EQ(expected, result);
}

TEST(Traversal, ExpandCurrent) {
  //  make a complete binary tree two levels deep (7 nodes)
  Vertex v0;
  Vertex v1, v2;
  Edge e1(v0, v1), e2(v0, v2);
  Vertex v3, v4, v5, v6;
  Edge e3(v1, v3), e4(v1, v4), e5(v2, v5), e6(v2, v6);

  // traverse two levels deep and accumulate results
  std::vector<Vertex> begin_input{v0};
  auto begin = Begin(begin_input);
  auto expand0 = begin.Expand(Expansion::Back, Direction::Out);
  auto expand1 = expand0.Expand(Expansion::Back, Direction::Out);

  // expected results
  std::unordered_set<Vertex> expected_expand0_vertices{v1, v2};
  std::unordered_set<Edge> expected_expand0_edges{e1, e2};
  std::unordered_set<Vertex> expected_expand1_vertices{v3, v4, v5, v6};
  std::unordered_set<Edge> expected_expand1_edges{e3, e4, e5, e6};

  expand1.Visit([&](Path &path){
    // we can't check that erasure on the first level because it only gets
    // erased the first time a path containing first level stuff gets visited
    expected_expand0_vertices.erase(expand0.CurrentVertex());
    expected_expand0_edges.erase(expand0.CurrentEdge());
    EXPECT_EQ(1, expected_expand1_vertices.erase(expand1.CurrentVertex()));
    EXPECT_EQ(1, expected_expand1_edges.erase(expand1.CurrentEdge()));
  });

  EXPECT_EQ(0, expected_expand0_vertices.size());
  EXPECT_EQ(0, expected_expand0_edges.size());
  EXPECT_EQ(0, expected_expand1_vertices.size());
  EXPECT_EQ(0, expected_expand1_edges.size());
}

TEST(Traversal, ExpandVariableLimits) {
  // make a chain
  Vertex v0;
  Vertex v1; Edge e1(v0, v1);
  Vertex v2; Edge e2(v1, v2);
  Vertex v3; Edge e3(v2, v3);
  Vertex v4; Edge e4(v3, v4);

  // all possible results
  std::vector<Path> all_possible{Path().Start(v0)};
  all_possible.emplace_back(Path(all_possible.back()).Append(e1, v1));
  all_possible.emplace_back(Path(all_possible.back()).Append(e2, v2));
  all_possible.emplace_back(Path(all_possible.back()).Append(e3, v3));
  all_possible.emplace_back(Path(all_possible.back()).Append(e4, v4));

  // default is one or more traversals
  auto result0 = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).ExpandVariable(Expansion::Back, Direction::Out));
  EXPECT_EQ(result0, std::unordered_set<Path>(all_possible.begin() + 1, all_possible.end()));

  // zero or more traversals
  auto result1 = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          ExpandVariable(Expansion::Back, Direction::Out, {}, {}, 0));
  EXPECT_EQ(result1, std::unordered_set<Path>(all_possible.begin(), all_possible.end()));

  // an exact number of traversals [2, 4)
  auto result2 = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          ExpandVariable(Expansion::Back, Direction::Out, {}, {}, 2, 4));
  EXPECT_EQ(result2, std::unordered_set<Path>(all_possible.begin() + 2, all_possible.begin() + 4));
}

TEST(Traversal, ExpandVariableFilter) {
  // make a chain with vertex labels and edge types with these values
  // (0)-[1]->(1)-[2]->(2)-[3]->(3)
  Vertex v0(0);
  Vertex v1(1); Edge e1(v0, v1, 1);
  Vertex v2(2); Edge e2(v1, v2, 2);
  Vertex v3(3); Edge e3(v2, v3, 3);

  // all possible paths
  std::vector<Path> all_possible{Path().Start(v0)};
  all_possible.emplace_back(Path(all_possible.back()).Append(e1, v1));
  all_possible.emplace_back(Path(all_possible.back()).Append(e2, v2));
  all_possible.emplace_back(Path(all_possible.back()).Append(e3, v3));

  // filter on edge type < 3
  auto result0 = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          ExpandVariable(Expansion::Back, Direction::Out, {},
                         [](const Edge &e) { return e.type_ < 3; }));
  EXPECT_EQ(result0, std::unordered_set<Path>(all_possible.begin() + 1, all_possible.begin() + 3));

  // filter on edge type > 1, expect nothing because the first edge is 1
  auto result1 = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          ExpandVariable(Expansion::Back, Direction::Out, {},
                         [](const Edge &e) { return e.type_ > 1; }));
  EXPECT_EQ(result1.size(), 0);

  // filter on vertex label > 1, expect last 2 results
  auto result2 = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          ExpandVariable(Expansion::Back, Direction::Out,
                         [](const Vertex &v) { return v.label_ > 1; }, {}));
  EXPECT_EQ(result2, std::unordered_set<Path>(all_possible.end() - 2, all_possible.end()));
}

TEST(Traversal, ExpandVariableCurrent) {
  // a chain
  Vertex v0;
  Vertex v1; Edge e1(v0, v1);
  Vertex v2; Edge e2(v1, v2);
  Vertex v3; Edge e3(v2, v3);

  // ensure that expanded vertices and all edge sets get visited as current
  std::unordered_set<Vertex> expected_vertices{v1, v2, v3};
  std::unordered_set<std::list<Edge>> expected_edge_lists{
      std::list<Edge>{e1},
      std::list<Edge>{e1, e2},
      std::list<Edge>{e1, e2, e3}
  };

  std::vector<Vertex> begin_input{v0};
  auto begin = Begin(begin_input);
  auto expand = begin.ExpandVariable(Expansion::Back, Direction::Out);

  expand.Visit([&](Path &path){
    EXPECT_EQ(1, expected_vertices.erase(expand.CurrentVertex()));
    EXPECT_EQ(1, expected_edge_lists.erase(expand.CurrentEdges()));
  });
}

TEST(Traversal, EnumExpansion) {
  // (v0)-[]->(v1)
  Vertex v0, v1;
  Edge e0(v0, v1);

  //  Expansion::Back
  auto result0= accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).Expand(Expansion::Back, Direction::Out));
  auto expected0 = std::unordered_set<Path>{Path().Start(v0).Append(e0, v1)};
  EXPECT_EQ(result0, expected0);

  //  Expansion::Front
  auto result1= accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).Expand(Expansion::Front, Direction::Out));
  auto expected1 = std::unordered_set<Path>{Path().Start(v0).Prepend(e0, v1)};
  EXPECT_EQ(result1, expected1);
}

TEST(Traversal, EnumDirection) {
  // (v0)-[]->(v1)-[]->(v2)
  Vertex v0, v1, v2;
  Edge e0(v0, v1), e1(v1, v2);

  //  Direction::Out
  auto result0= accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v1}).Expand(Expansion::Back, Direction::Out));
  auto expected0 = std::unordered_set<Path>{Path().Start(v1).Append(e1, v2)};
  EXPECT_EQ(result0, expected0);

  //  Direction::In
  auto result1= accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v1}).Expand(Expansion::Back, Direction::In));
  auto expected1 = std::unordered_set<Path>{Path().Start(v1).Append(e0, v0)};
  EXPECT_EQ(result1, expected1);

  // Direction::Both
  auto result2= accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v1}).Expand(Expansion::Back, Direction::Both));
  auto expected2 = std::unordered_set<Path>{
      Path().Start(v1).Append(e0, v0),
      Path().Start(v1).Append(e1, v2)
  };
  EXPECT_EQ(result2, expected2);
}

TEST(Traversal, EnumUniqueness) {
  // make a (v0)-[]->(v1), where (v0) has a recursive relationship to self
  Vertex v0, v1;
  Edge e0(v0, v0), e1(v0, v1);

  //  Uniqueness::Edge
  auto result0 = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          Expand(Expansion::Back, Direction::Out, {}, {}, Uniqueness::Edge).
          Expand(Expansion::Back, Direction::Out));
  auto expected0 = std::unordered_set<Path>{
      Path().Start(v0).Append(e0, v0).Append(e1, v1)
  };
  EXPECT_EQ(result0, expected0);

  //  Uniqueness::Vertex
  auto result1 = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          Expand(Expansion::Back, Direction::Out, {}, {}, Uniqueness::Vertex).
          Expand(Expansion::Back, Direction::Out));
  EXPECT_EQ(result1.size(), 0);

  // Uniqueness::None
  auto result2 = accumulate_visited<Path>(
      Begin(std::vector<Vertex>{v0}).
          Expand(Expansion::Back, Direction::Out, {}, {}, Uniqueness::None).
          Expand(Expansion::Back, Direction::Out));
  auto expected2 = std::unordered_set<Path>{
      Path().Start(v0).Append(e0, v0).Append(e1, v1),
      Path().Start(v0).Append(e0, v0).Append(e0, v0)
  };
  EXPECT_EQ(result2, expected2);
}

TEST(Traversal, Cartesian) {
  // first make two simple trees
  Vertex v0, v1, v2;
  Edge e1(v0, v1), e2(v0, v1);
  Vertex v3, v4, v5;
  Edge e3(v3, v4), e4(v3, v5);

  // now accumulate all paths from three starting points
  std::vector<Vertex> begin0{v0}, begin1{v3};
  auto paths0 = Begin(begin0).Expand(Expansion::Back, Direction::Out);
  auto paths1 = Begin(begin1).Expand(Expansion::Back, Direction::Out);
  auto paths0_set = accumulate_visited<Path>(paths0);
  auto paths1_set = accumulate_visited<Path>(paths1);
  EXPECT_EQ(2, paths0_set.size());
  EXPECT_EQ(2, paths1_set.size());

  // now make a cartesian product of (0, 1, 0) and remember the results
  std::unordered_set<Paths> expected;
  for (auto &path1 : paths0_set)
    for (auto &path2 : paths1_set)
      for (auto &path3: paths0_set) {
        Paths paths;
        paths.emplace_back(std::ref(const_cast<Path&>(path1)));
        paths.emplace_back(std::ref(const_cast<Path&>(path2)));
        paths.emplace_back(std::ref(const_cast<Path&>(path3)));
        expected.insert(paths);
      }
  EXPECT_EQ(8, expected.size());

  // now use the Cartesian API
  auto paths2 = Begin(begin0).Expand(Expansion::Back, Direction::Out);
  Cartesian(paths0, paths1, paths2).Visit([&expected](Paths &p) {
    EXPECT_EQ(1, expected.erase(p));
  });

  EXPECT_EQ(0, expected.size());
}

TEST(Traversal, UniquenessSubgroups) {
  // graph is (v1)<-[]-(v0)-[]->(v2)
  Vertex v0, v1, v2;
  Edge e1(v0, v1), e2(v0, v1);
  std::vector<Vertex> begin_v{v0};

  // counts the number of visits to the visitable
  auto visit_counter = [](const auto &visitable) {
    auto r_val = 0;
    visitable.Visit([&r_val](const auto &x) {r_val++;});
    return r_val;
  };

  // make a few experiments with different uniqueness sharing

  // no uniqueness sharing
  {
    auto paths0 = Begin(begin_v).Expand(Expansion::Back, Direction::Out);
    auto paths1 = Begin(begin_v).Expand(Expansion::Back, Direction::Out);
    EXPECT_EQ(4, visit_counter(Cartesian(paths0, paths1)));
  }

  // edge uniqueness sharing
  {
    auto paths0 = Begin(begin_v).Expand(Expansion::Back, Direction::Out, {}, {}, Uniqueness::Edge);
    auto paths1 = Begin(begin_v).Expand(Expansion::Back, Direction::Out, {}, {}, Uniqueness::Edge);
    paths1.UniquenessGroup().Add(paths0.UniquenessGroup());
    EXPECT_EQ(2, visit_counter(Cartesian(paths0, paths1)));
  }

  // vertex uniqueness sharing
  {
    auto paths0 = Begin(begin_v).Expand(Expansion::Back, Direction::Out, {}, {}, Uniqueness::Vertex);
    auto paths1 = Begin(begin_v).Expand(Expansion::Back, Direction::Out, {}, {}, Uniqueness::Vertex);
    paths1.UniquenessGroup().Add(paths0.UniquenessGroup());
    EXPECT_EQ(0, visit_counter(Cartesian(paths0, paths1)));
  }
}
