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
 * Returns the index of the best (highest scored) worker
 * for the given node. If there are multiple workers with
 * the best score, node prefers to remain on the same worker
 * (if among the best), or one is chosen at random.
 *
 * @param distributed - the distributed system.
 * @param node - the node which is being evaluated.
 * @param penalties - a vector of penalties (per worker).
 * @param current_worker - the worker on which the given
 *  node is currently residing.
 * @return - std::pair<int, std::vector<double>> which is a
 * pair of (best worker, score_per_worker).
 */
auto BestWorker(const Distributed &distributed, const Node &node,
                const std::vector<double> &penalties, int current_worker) {
  // scores per worker
  std::vector<double> scores(distributed.WorkerCount(), 0.0);

  for (auto &edge : node.edges_in()) scores[edge.worker_id_] += 1.0;
  for (auto &edge : node.edges_out()) scores[edge.worker_id_] += 1.0;

  for (int worker = 0; worker < distributed.WorkerCount(); ++worker) {
    // normalize contribution of worker over neighbourhood size
    scores[worker] /= node.edges_out().size() + node.edges_in().size();
    // add balancing penalty
    scores[worker] -= penalties[worker];
  }

  // pick the best destination, but prefer to stay if you can
  size_t destination = MaxRandom(scores);
  if (scores[current_worker] == scores[destination])
    destination = current_worker;

  return std::make_pair(destination, scores);
}

/** Indication if Spinner worker penality is calculated based on
 * vertex or edge worker cardinalities */
enum class PenaltyType { Vertex, Edge };

/** Calcualtes Spinner penalties for workers in the given
 * distributed system. */
auto Penalties(const Distributed &distributed,
               PenaltyType penalty_type = PenaltyType::Edge) {
  std::vector<double> penalties;
  int64_t total_count{0};

  for (const auto &worker : distributed) {
    int64_t worker_count{0};
    switch (penalty_type) {
      case PenaltyType::Vertex:
        worker_count += worker.NodeCount();
        break;
      case PenaltyType::Edge:
        for (const auto &node_kv : worker) {
          // Spinner counts the edges on a worker as the sum
          // of degrees of nodes on that worker. In that sense
          // both incoming and outgoing edges are individually
          // added...
          worker_count += node_kv.second.edges_out().size();
          worker_count += node_kv.second.edges_in().size();
        }
        break;
    }
    total_count += worker_count;
    penalties.emplace_back(worker_count);
  }

  for (auto &penalty : penalties)
    penalty /= c * total_count / distributed.WorkerCount();

  return penalties;
}

/** Do one spinner step (modifying the given distributed) */
void PerformSpinnerStep(Distributed &distributed) {
  auto penalties = Penalties(distributed);

  // here a strategy can be injected for limiting
  // the number of movements performed in one step.
  // limiting could be based on (for example):
  //  - limiting the number of movements per worker
  //  - limiting only to movements that are above
  //    a treshold (score improvement or something)
  //  - not executing on all the workers (also prevents
  //    oscilations)
  //
  // in the first implementation just accumulate all
  // the movements and execute together.

  // relocation info: contains the address of the Node
  // that needs to relocate and it's destination worker
  std::vector<std::pair<GlobalAddress, int>> movements;

  for (const Worker &worker : distributed)
    for (const auto &gid_node_pair : worker) {
      // (best destination, scores) pair for node
      std::pair<int, std::vector<double>> destination_scores =
          BestWorker(distributed, gid_node_pair.second, penalties, worker.id_);
      if (destination_scores.first != worker.id_)
        movements.emplace_back(GlobalAddress(worker.id_, gid_node_pair.first),
                               destination_scores.first);
    }

  // execute movements. it is likely that in the real system
  // this will need to happen as a single db transaction
  for (const auto &m : movements) distributed.MoveNode(m.first, m.second);
}
}  // namespace spinner
