#include <random>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "json/json.hpp"

#include "snapshot_generation/snapshot_writer.hpp"

DEFINE_int32(num_workers, 1,
             "Number of distributed workers (including master)");
DEFINE_string(dir, "tmp",
              "Directory for storing workers durability directories.");
DEFINE_string(config, "", "Path to config JSON file");

/**
 * Config file should be defined as follows:
 * {
 *   "scale": 10,
 *   "edge_factor": 16,
 *   "probabilities": [0.57, 0.19, 0.19]
 * }
 */

using namespace snapshot_generation;

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  nlohmann::json config;
  {
    std::ifstream config_file(FLAGS_config);
    config_file >> config;
  }

  int64_t N = 1LL << config["scale"].get<int>();
  int64_t M = config["edge_factor"].get<double>() * N;

  const double A = config["probabilities"][0];
  const double B = config["probabilities"][1];
  const double C = config["probabilities"][2];
  const double D = 1 - (A + B + C);

  std::vector<std::pair<int64_t, int64_t>> edges(M);

  std::mt19937_64 gen(std::random_device{}());
  std::uniform_real_distribution<double> dist(0, 1);

  for (int i = 0; i < M; ++i) {
    for (int j = 0; j < config["scale"]; ++j) {
      if (dist(gen) > A + B) {
        edges[i].first |= 1 << j;
        if (dist(gen) > C / (C + D)) {
          edges[i].second |= 1 << j;
        }
      } else {
        if (dist(gen) > A / (A + B)) {
          edges[i].second |= 1 << j;
        }
      }
    }
  }

  std::vector<int64_t> vertex_labels(N);
  std::iota(vertex_labels.begin(), vertex_labels.end(), 0);
  std::random_shuffle(vertex_labels.begin(), vertex_labels.end());

  GraphState state(FLAGS_num_workers);

  state.CreateIndex("Node", "id");

  std::vector<gid::Gid> vertices;
  vertices.reserve(N);

  for (int i = 0; i < N; ++i) {
    vertices.emplace_back(state.CreateNode(i % FLAGS_num_workers, {"Node"},
                                           {{"id", vertex_labels[i]}}));
  }

  std::random_shuffle(edges.begin(), edges.end());
  for (int i = 0; i < M; ++i) {
    auto e = edges[i];
    VLOG(1) << vertex_labels[e.first] << " " << vertex_labels[e.second];
    state.CreateEdge(vertices[e.first], vertices[e.second], "Edge", {});
  }

  LOG(INFO) << fmt::format("nodes = {}, edges = {}", N, M);
  WriteToSnapshot(state, FLAGS_dir);

  return 0;
}
