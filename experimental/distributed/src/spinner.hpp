#include <algorithm>
#include <cassert>
#include <cstddef>
#include <experimental/tuple>
#include <iostream>
#include <numeric>
#include <random>
#include <tuple>
#include <vector>

#include "graph.hpp"

namespace spinner {
// const for balancing penalty
double c = 2.0;

/**
 * Returns the index of the maximum score in the given vector.
 * If there are multiple minimums, one is chosen at random.
 */
auto MaxRandom(const std::vector<double> &scores) {
  std::vector<size_t> best_indices;
  double current_max = std::numeric_limits<double>::lowest();

  for (size_t ind = 0; ind < scores.size(); ind++) {
    if (scores[ind] > current_max) {
      current_max = scores[ind];
      best_indices.clear();
    }
    if (scores[ind] == current_max) {
      best_indices.emplace_back(ind);
    }
  }

  return best_indices[rand() % best_indices.size()];
}

/**
 * Returns the index of the best (highest scored) mnode
 * for the given vertex. If there are multiple mnodes with
 * the best score, vertex prefers to remain on the same mnode
 * (if among the best), or one is chosen at random.
 *
 * @param distributed - the distributed system.
 * @param vertex - the vertex which is being evaluated.
 * @param penalties - a vector of penalties (per mnode).
 * @param current_mnode - the mnode on which the given
 *  vertex is currently residing.
 * @return - std::pair<int, std::vector<double>> which is a
 * pair of (best mnode, score_per_mnode).
 */
auto BestMnode(const Distributed &distributed, const Vertex &vertex,
                const std::vector<double> &penalties, int current_mnode) {
  // scores per mnode
  std::vector<double> scores(distributed.MnodeCount(), 0.0);

  for (auto &edge : vertex.edges_in()) scores[edge.cur_mnid_] += 1.0;
  for (auto &edge : vertex.edges_out()) scores[edge.cur_mnid_] += 1.0;

  for (int mnode = 0; mnode < distributed.MnodeCount(); ++mnode) {
    // normalize contribution of mnode over neighbourhood size
    scores[mnode] /= vertex.edges_out().size() + vertex.edges_in().size();
    // add balancing penalty
    scores[mnode] -= penalties[mnode];
  }

  // pick the best destination, but prefer to stay if you can
  size_t destination = MaxRandom(scores);
  if (scores[current_mnode] == scores[destination])
    destination = current_mnode;

  return std::make_pair(destination, scores);
}

/** Indication if Spinner mnode penality is calculated based on
 * vertex or edge mnode cardinalities */
enum class PenaltyType { Vertex, Edge };

/** Calcualtes Spinner penalties for mnodes in the given
 * distributed system. */
auto Penalties(const Distributed &distributed,
               PenaltyType penalty_type = PenaltyType::Edge) {
  std::vector<double> penalties;
  int64_t total_count{0};

  for (const auto &mnode : distributed) {
    int64_t mnode_count{0};
    switch (penalty_type) {
      case PenaltyType::Vertex:
        mnode_count += mnode.VertexCount();
        break;
      case PenaltyType::Edge:
        for (const auto &vertex_kv : mnode) {
          // Spinner counts the edges on a mnode as the sum
          // of degrees of vertices on that mnode. In that sense
          // both incoming and outgoing edges are individually
          // added...
          mnode_count += vertex_kv.second.edges_out().size();
          mnode_count += vertex_kv.second.edges_in().size();
        }
        break;
    }
    total_count += mnode_count;
    penalties.emplace_back(mnode_count);
  }

  for (auto &penalty : penalties)
    penalty /= c * total_count / distributed.MnodeCount();

  return penalties;
}

/** Do one spinner step (modifying the given distributed) */
void PerformSpinnerStep(Distributed &distributed) {
  auto penalties = Penalties(distributed);

  // here a strategy can be injected for limiting
  // the number of movements performed in one step.
  // limiting could be based on (for example):
  //  - limiting the number of movements per mnode
  //  - limiting only to movements that are above
  //    a treshold (score improvement or something)
  //  - not executing on all the mnodes (also prevents
  //    oscilations)
  //
  // in the first implementation just accumulate all
  // the movements and execute together.

  // relocation info: contains the address of the Vertex
  // that needs to relocate and it's destination mnode
  std::vector<std::pair<GlobalVertAddress, int>> movements;

  for (const ShardedStorage &mnode : distributed)
    for (const auto &gid_vertex_pair : mnode) {
      // (best destination, scores) pair for vertex
      std::pair<int, std::vector<double>> destination_scores =
          BestMnode(distributed, gid_vertex_pair.second, penalties, mnode.mnid_);
      if (destination_scores.first != mnode.mnid_)
        movements.emplace_back(GlobalVertAddress(mnode.mnid_, gid_vertex_pair.first),
                               destination_scores.first);
    }

  // execute movements. it is likely that in the real system
  // this will need to happen as a single db transaction
  for (const auto &m : movements) distributed.MoveVertex(m.first, m.second);
}
}  // namespace spinner
