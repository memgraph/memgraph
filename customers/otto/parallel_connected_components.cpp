#include <algorithm>
#include <limits>
#include <mutex>
#include <random>
#include <set>
#include <stack>
#include <thread>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "data_structures/union_find.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "storage/property_value.hpp"
#include "threading/sync/spinlock.hpp"
#include "utils/bound.hpp"
#include "utils/timer.hpp"

DEFINE_int32(thread_count, 1, "Number of threads");
DEFINE_int32(vertex_count, 1000, "Number of vertices");
DEFINE_int32(edge_count, 1000, "Number of edges");
DECLARE_int32(gc_cycle_sec);

static const std::string kLabel{"kLabel"};
static const std::string kProperty{"kProperty"};

void GenerateGraph(database::GraphDb &db) {
  {
    database::GraphDbAccessor dba{db};
    dba.BuildIndex(dba.Label(kLabel), dba.Property(kProperty));
    dba.Commit();
  }

  // Randomize the sequence of IDs of created vertices and edges to simulate
  // real-world lack of locality.
  auto make_id_vector = [](size_t size) {
    gid::Generator generator{0};
    std::vector<gid::Gid> ids(size);
    for (size_t i = 0; i < size; ++i)
      ids[i] = generator.Next(std::experimental::nullopt);
    std::random_shuffle(ids.begin(), ids.end());
    return ids;
  };

  std::vector<VertexAccessor> vertices;
  vertices.reserve(FLAGS_vertex_count);
  {
    CHECK(FLAGS_vertex_count % FLAGS_thread_count == 0)
        << "Thread count must be a factor of vertex count";
    LOG(INFO) << "Generating " << FLAGS_vertex_count << " vertices...";
    utils::Timer timer;
    auto vertex_ids = make_id_vector(FLAGS_vertex_count);

    std::vector<std::thread> threads;
    SpinLock vertices_lock;
    for (int i = 0; i < FLAGS_thread_count; ++i) {
      threads.emplace_back([&db, &vertex_ids, &vertices, &vertices_lock, i]() {
        database::GraphDbAccessor dba{db};
        auto label = dba.Label(kLabel);
        auto property = dba.Property(kProperty);
        auto batch_size = FLAGS_vertex_count / FLAGS_thread_count;
        for (int j = i * batch_size; j < (i + 1) * batch_size; ++j) {
          auto vertex = dba.InsertVertex(vertex_ids[j]);
          vertex.add_label(label);
          vertex.PropsSet(property, static_cast<int64_t>(vertex_ids[j]));
          vertices_lock.lock();
          vertices.emplace_back(vertex);
          vertices_lock.unlock();
        }
        dba.Commit();
      });
    }
    for (auto &t : threads) t.join();
    LOG(INFO) << "Generated " << FLAGS_vertex_count << " vertices in "
              << timer.Elapsed().count() << " seconds.";
  }
  {
    database::GraphDbAccessor dba{db};
    for (int i = 0; i < FLAGS_vertex_count; ++i)
      vertices[i] = *dba.Transfer(vertices[i]);

    LOG(INFO) << "Generating " << FLAGS_edge_count << " edges...";
    auto edge_ids = make_id_vector(FLAGS_edge_count);
    std::mt19937 pseudo_rand_gen{std::random_device{}()};
    std::uniform_int_distribution<> rand_dist{0, FLAGS_vertex_count - 1};
    auto edge_type = dba.EdgeType("edge");
    utils::Timer timer;
    for (int i = 0; i < FLAGS_edge_count; ++i)
      dba.InsertEdge(vertices[rand_dist(pseudo_rand_gen)],
                     vertices[rand_dist(pseudo_rand_gen)], edge_type,
                     edge_ids[i]);
    dba.Commit();
    LOG(INFO) << "Generated " << FLAGS_edge_count << " edges in "
              << timer.Elapsed().count() << " seconds.";
  }
}

auto EdgeIteration(database::GraphDb &db) {
  database::GraphDbAccessor dba{db};
  int64_t sum{0};
  for (auto edge : dba.Edges(false)) sum += edge.from().gid() + edge.to().gid();
  return sum;
}

auto VertexIteration(database::GraphDb &db) {
  database::GraphDbAccessor dba{db};
  int64_t sum{0};
  for (auto v : dba.Vertices(false))
    for (auto e : v.out()) sum += e.gid() + e.to().gid();
  return sum;
}

auto ConnectedComponentsEdges(database::GraphDb &db) {
  UnionFind<int64_t> connectivity{FLAGS_vertex_count};
  database::GraphDbAccessor dba{db};
  for (auto edge : dba.Edges(false))
    connectivity.Connect(edge.from().gid(), edge.to().gid());
  return connectivity.Size();
}

auto ConnectedComponentsVertices(database::GraphDb &db) {
  UnionFind<int64_t> connectivity{FLAGS_vertex_count};
  database::GraphDbAccessor dba{db};
  for (auto from : dba.Vertices(false)) {
    for (auto out_edge : from.out())
      connectivity.Connect(from.gid(), out_edge.to().gid());
  }
  return connectivity.Size();
}

auto ConnectedComponentsVerticesParallel(database::GraphDb &db) {
  UnionFind<int64_t> connectivity{FLAGS_vertex_count};
  SpinLock connectivity_lock;

  // Define bounds of vertex IDs for each thread to use.
  std::vector<PropertyValue> bounds;
  for (int64_t i = 0; i < FLAGS_thread_count; ++i)
    bounds.emplace_back(i * FLAGS_vertex_count / FLAGS_thread_count);
  bounds.emplace_back(std::numeric_limits<int64_t>::max());

  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_thread_count; ++i) {
    threads.emplace_back(
        [&connectivity, &connectivity_lock, &bounds, &db, i]() {
          database::GraphDbAccessor dba{db};
          for (auto from :
               dba.Vertices(dba.Label(kLabel), dba.Property(kProperty),
                            utils::MakeBoundInclusive(bounds[i]),
                            utils::MakeBoundExclusive(bounds[i + 1]), false)) {
            for (auto out_edge : from.out()) {
              std::lock_guard<SpinLock> lock{connectivity_lock};
              connectivity.Connect(from.gid(), out_edge.to().gid());
            }
          }
        });
  }
  for (auto &t : threads) t.join();
  return connectivity.Size();
}

auto Expansion(database::GraphDb &db) {
  std::vector<int> component_ids(FLAGS_vertex_count, -1);
  int next_component_id{0};
  std::stack<VertexAccessor> expansion_stack;
  database::GraphDbAccessor dba{db};
  for (auto v : dba.Vertices(false)) {
    if (component_ids[v.gid()] != -1) continue;
    auto component_id = next_component_id++;
    expansion_stack.push(v);
    while (!expansion_stack.empty()) {
      auto next_v = expansion_stack.top();
      expansion_stack.pop();
      if (component_ids[next_v.gid()] != -1) continue;
      component_ids[next_v.gid()] = component_id;
      for (auto e : next_v.out()) expansion_stack.push(e.to());
      for (auto e : next_v.in()) expansion_stack.push(e.from());
    }
  }

  return next_component_id;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_gc_cycle_sec = -1;

  database::SingleNode db;
  GenerateGraph(db);
  auto timed_call = [&db](auto callable, const std::string &descr) {
    LOG(INFO) << "Running " << descr << "...";
    utils::Timer timer;
    auto result = callable(db);
    LOG(INFO) << "\tDone in " << timer.Elapsed().count()
              << " seconds, result: " << result;
  };
  timed_call(EdgeIteration, "Edge iteration");
  timed_call(VertexIteration, "Vertex iteration");
  timed_call(ConnectedComponentsEdges, "Connected components - Edges");
  timed_call(ConnectedComponentsVertices, "Connected components - Vertices");
  timed_call(ConnectedComponentsVerticesParallel,
             "Parallel connected components - Vertices");
  timed_call(Expansion, "Expansion");

  return 0;
}
