#include <iostream>
#include <mutex>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "database/graph_db.hpp"
#include "query/console.hpp"
#include "query/interpreter.hpp"
#include "utils/random_graph_generator.hpp"

/** A graph-generation progress reporter */
class ProgressReporter {
 public:
  ProgressReporter(int64_t node_count, int64_t edge_count, int64_t skip)
      : node_count_(node_count), edge_count_(edge_count), skip_(skip) {}

  void operator()(utils::RandomGraphGenerator &rgg) {
    auto vc = rgg.VertexCount();
    auto ec = rgg.EdgeCount();
    bool last = ec + vc == node_count_ + edge_count_;
    auto percent = std::lround(100. * (vc + ec) / (node_count_ + edge_count_));
    if (last || (vc + ec) % skip_ == 0) {
      std::lock_guard<std::mutex> lock(mutex_);
      std::cout << "\rCreated " << rgg.VertexCount() << " vertices and "
                << rgg.EdgeCount() << " edges (" << percent
                << "% of all elements)";
      std::flush(std::cout);
    }

    if (last) std::cout << std::endl;
  }

 private:
  // the desired counts of nodes and edges
  const int64_t node_count_;
  const int64_t edge_count_;

  // how many notifications we skip between each report
  const int64_t skip_;

  // std output synchronization
  std::mutex mutex_{};
};

void random_generate(database::GraphDb &db, int64_t node_count,
                     int64_t edge_count) {
  utils::RandomGraphGenerator generator(db);
  ProgressReporter reporter(node_count, edge_count,
                            std::max(1l, (node_count + edge_count) / 100));
  generator.AddProgressListener([&reporter](auto &rgg) { reporter(rgg); });

  utils::Timer generation_timer;
  generator.AddVertices(node_count, {"Person"}, 4);
  generator.AddEdges(edge_count, "Friend", 7);
  generator.SetVertexProperty<int>("age", utils::RandomIntGenerator(3, 60));
  generator.SetVertexProperty<int>("height",
                                   utils::RandomIntGenerator(120, 200));
  std::cout << "Generation done in " << generation_timer.Elapsed().count()
            << " seconds" << std::endl;
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // parse the first cmd line argument as the count of nodes to randomly create
  int node_count = argc > 1 ? std::stoi(argv[1]) : 0;
  int edge_count = argc > 2 ? std::stoi(argv[2]) : 0;

  // TODO switch to GFlags, once finally available
  if (argc > 3) google::InitGoogleLogging(argv[0]);

  database::SingleNode db;
  std::cout << "Generating graph..." << std::endl;
  //  fill_db;
  random_generate(db, node_count, edge_count);
  query::Repl(db);
  return 0;
}
