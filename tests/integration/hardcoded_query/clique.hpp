#include <algorithm>
#include <bitset>
#include <iostream>
#include <string>

#include "data_structures/bitset/static_bitset.hpp"
#include "query/backend/cpp/typed_value.hpp"
#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"
#include "utils/assert.hpp"
#include "query/parameters.hpp"

using std::cout;
using std::endl;

// General Query: MATCH
//  (a:garment)-[:default_outfit]-(b:garment)-[:default_outfit]-(c:garment)-[:default_outfit]-(d:garment)-[:default_outfit]-(a:garment)-[:default_outfit]-(c:garment),
//  (b:garment)-[:default_outfit]-(d:garment), (e:profile {profile_id: 112,
//  partner_id: 55})-[s1:score]-(a:garment),(e:profile {profile_id: 112,
//  partner_id: 55})-[s2:score]-(b:garment), (e:profile {profile_id: 112,
//  partner_id: 55})-[s3:score]-(c:garment), (e:profile {profile_id: 112,
//  partner_id: 55})-[s4:score]-(d:garment) WHERE a.garment_id=1234 RETURN
//  a.garment_id,b.garment_id,c.garment_id,d.garment_id,
//  s1.score+s2.score+s3.score+s4.score ORDER BY
//  s1.score+s2.score+s3.score+s4.score DESC LIMIT 10

enum CliqueQuery { SCORE_AND_LIMIT, FIND_ALL };

bool run_general_query(GraphDbAccessor &db_accessor,
                       const Parameters &args, Stream &stream,
                       enum CliqueQuery query_type) {
  if (query_type == CliqueQuery::FIND_ALL)
    stream.write_fields(
        {"a.garment_id", "b.garment_id", "c.garment_id", "d.garment_id"});
  else
    stream.write_fields({"a.garment_id", "b.garment_id", "c.garment_id",
                         "d.garment_id", "score"});
  // TODO dgleich: this code is very inefficient as it first makes a copy
  // of all the vertices/edges, and filters aftwarwards. I warned about this
  // happening in code review!!!
  auto vertices_iterator = db_accessor.vertices();
  auto edge_iterator = db_accessor.edges();
  std::vector<VertexAccessor> vertices(vertices_iterator.begin(),
                                       vertices_iterator.end());
  std::vector<EdgeAccessor> edges(edge_iterator.begin(), edge_iterator.end());

  std::vector<VertexAccessor *> vertices_indexed;
  std::vector<EdgeAccessor *> edges_indexed;

  int profile_index = -1;
  for (int i = 0; i < (int)vertices.size(); ++i) {
    if (vertices[i].has_label(db_accessor.label("garment")))
      vertices_indexed.push_back(&vertices[i]);
    if (query_type == CliqueQuery::SCORE_AND_LIMIT &&
        vertices[i].has_label(db_accessor.label("profile"))) {
      auto has_prop =
          vertices[i].PropsAt(db_accessor.property("profile_id")) == args.At(0);
      if (has_prop.type() == TypedValue::Type::Null) continue;
      if (has_prop.Value<bool>() == false) continue;
      has_prop =
          vertices[i].PropsAt(db_accessor.property("partner_id")) == args.At(1);
      if (has_prop.type() == TypedValue::Type::Null) continue;
      if (has_prop.Value<bool>() == false) continue;
      profile_index = i;
    }
  }
  for (int i = 0; i < (int)edges.size(); ++i) {
    if (edges[i].edge_type() == db_accessor.edge_type("default_outfit") ||
        edges[i].edge_type() == db_accessor.edge_type("score"))
      edges_indexed.push_back(&edges[i]);
  }
  const int n = vertices_indexed.size();
  auto cmp_vertex = [](const VertexAccessor *a,
                       const VertexAccessor *b) -> bool { return *a < *b; };
  auto cmp_edge = [](const EdgeAccessor *a, const EdgeAccessor *b) -> bool {
    if (a->from() != b->from()) return a->from() < b->from();
    return a->to() < b->to();
  };
  /**
   * Index of vertex in a list of vertices_indexed.
   * @param v VertexAccessor to a vertex.
   * @return position of vertex or -1 if it doesn't exist.
   */
  auto query = [&vertices_indexed,
                &cmp_vertex](const VertexAccessor &v) -> int {
    int pos = lower_bound(vertices_indexed.begin(), vertices_indexed.end(), &v,
                          cmp_vertex) -
              vertices_indexed.begin();
    if (pos == (int)vertices_indexed.size() || *vertices_indexed[pos] != v)
      return -1;
    return pos;
  };
  /**
   * Update bitset of neighbours. Set bit to 1 for index of every vertex
   * endpoint of edges with type default_outfit.
   * @param bitset bitset to update.
   * @param edges edges from which to update bitset.
   */
  auto update = [&db_accessor, &query](Bitset<int64_t> &bitset, auto &&edges) {
    for (auto e : edges) {
      if (e.edge_type() != db_accessor.edge_type("default_outfit")) continue;
      const int from = query(e.from());
      const int to = query(e.to());
      if (from == -1 || to == -1) continue;
      bitset.Set(from);
      bitset.Set(to);
    }
  };
  std::sort(vertices_indexed.begin(), vertices_indexed.end(), cmp_vertex);
  std::sort(edges_indexed.begin(), edges_indexed.end(), cmp_edge);
  std::vector<Bitset<int64_t>> connected;
  for (int i = 0; i < n; ++i) {
    Bitset<int64_t> connected_to(n);
    update(connected_to, vertices_indexed[i]->in());
    update(connected_to, vertices_indexed[i]->out());
    connected.push_back(connected_to);
  }
  std::vector<std::vector<int>> results;
  for (int i = 0; i < n; ++i) {
    const VertexAccessor v = *vertices_indexed[i];
    auto cmp_res = v.PropsAt(db_accessor.property("garment_id")) ==
                   args.At(query_type == CliqueQuery::SCORE_AND_LIMIT ? 8 : 0);
    if (cmp_res.type() != TypedValue::Type::Bool) continue;
    if (cmp_res.Value<bool>() != true) continue;
    auto neigh = connected[i].Ones();
    for (int j : neigh) {
      if (j == i) continue;
      for (int k : connected[j].Intersect(connected[i]).Ones()) {
        if (k == i) continue;
        if (k == j) continue;
        Bitset<int64_t> intersection = connected[j].Intersect(connected[k]);
        for (int l : intersection.Ones()) {
          if (l == i) continue;
          if (l == j) continue;
          if (l == k) continue;
          if (connected[l].At(i) == false) continue;
          results.push_back({i, j, k, l});
        }
      }
    }
  }
  /**
   * Get edge index of edge associated with two vertices.
   * @param first one vertex endpoint
   * @param second other vertex endpoint
   * @return EdgeAccessor* if it exists, nullptr otherwise.
   */
  auto get_edge = [&edges_indexed](
      const VertexAccessor &first,
      const VertexAccessor &second) -> EdgeAccessor * {
    auto cmp_edge_to_pair = [](
        const EdgeAccessor *edge,
        const pair<const VertexAccessor *, const VertexAccessor *> &e) -> bool {
      if (edge->from() != *e.first) return edge->from() < *e.first;
      if (edge->to() != *e.second) return edge->to() < *e.second;
      return false;
    };
    auto pos = lower_bound(edges_indexed.begin(), edges_indexed.end(),
                           std::make_pair(&first, &second), cmp_edge_to_pair) -
               edges_indexed.begin();
    if (pos != (int)edges_indexed.size() &&
        edges_indexed[pos]->from() == first &&
        edges_indexed[pos]->to() == second)
      return edges_indexed[pos];
    pos = lower_bound(edges_indexed.begin(), edges_indexed.end(),
                      std::make_pair(&second, &first), cmp_edge_to_pair) -
          edges_indexed.begin();
    if (pos != (int)edges_indexed.size() &&
        edges_indexed[pos]->from() == second &&
        edges_indexed[pos]->to() == first)
      return edges_indexed[pos];
    debug_assert(false, "Edge doesn't exist.");
    return nullptr;
  };

  /**
   * Calculate score of clique by profile_index if it exists.
   * @param V index of clique vertices in vertices_indexed.
   * @return score if profile_index exists, else 0.
   */
  auto calc_score = [&db_accessor, &vertices, &profile_index, &vertices_indexed,
                     &get_edge](const std::vector<int> &V) -> int {
    int res = 0;
    if (profile_index == -1) return 0;
    for (auto x : V) {
      auto edge = get_edge(vertices[profile_index], *vertices_indexed[x]);
      if (edge == nullptr) continue;
      auto prop = TypedValue(edge->PropsAt(db_accessor.property("score")));
      if (prop.type() == TypedValue::Type::Int) res += prop.Value<int>();
    }
    return res;
  };
  if (query_type == CliqueQuery::SCORE_AND_LIMIT) {
    auto cmp_results = [&calc_score](const std::vector<int> &first,
                                     const std::vector<int> &second) {
      return calc_score(first) < calc_score(second);
    };
    sort(results.begin(), results.end(), cmp_results);
    reverse(results.begin(), results.end());
  }
  const int limit = query_type == CliqueQuery::SCORE_AND_LIMIT
                        ? args.At((int)args.Size() - 1).Value<int>()
                        : (int)results.size();
  for (int i = 0; i < std::min(limit, (int)results.size()); ++i) {
    stream.write_record();
    stream.write_list_header(query_type == CliqueQuery::SCORE_AND_LIMIT ? 5
                                                                        : 4);
    for (auto x : results[i]) {
      stream.write(vertices_indexed[x]
                       ->PropsAt(db_accessor.property("garment_id"))
                       .Value<int>());
    }
    if (query_type == CliqueQuery::SCORE_AND_LIMIT)
      stream.write(calc_score(results[i]));
  }
  stream.write_meta("r");
  db_accessor.commit();
  return true;
}
