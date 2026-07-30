// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
// Online node2vec query module. Maintains node embeddings incrementally as new
// edges arrive (typically via a BEFORE COMMIT trigger calling `update`).

#include <mgp.hpp>

#include <algorithm>
#include <ctime>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <node2vec/word2vec.hpp>

#include "algorithm/stream_walk_updater.hpp"

namespace {

constexpr const char *kResultMessage = "message";
constexpr const char *kResultNode = "node";
constexpr const char *kResultEmbedding = "embedding";
constexpr const char *kResultName = "name";
constexpr const char *kResultValue = "value";

// Persistent module-global state, shared across procedure invocations.
struct OnlineContext {
  std::unique_ptr<node2vec_alg::StreamWalkUpdater> updater;
  std::unique_ptr<node2vec_alg::Word2Vec> learner;

  bool IsInitialized() const { return updater != nullptr && learner != nullptr; }

  void Reset() {
    updater.reset();
    learner.reset();
  }
};

OnlineContext g_ctx;  // NOSONAR: intentional mutable module state, persisted across procedure calls
std::mutex g_mtx;     // NOSONAR: guards g_ctx; a mutex cannot be const

void CheckEnterprise() {
  if (!mgp_is_enterprise_valid()) {
    throw std::runtime_error("To use node2vec online module you need a valid enterprise license.");
  }
}

std::vector<mgp::Value> CollectArgs(mgp_list *args) {
  std::vector<mgp::Value> arguments;
  const size_t n = mgp::list_size(args);
  arguments.reserve(n);
  for (size_t i = 0; i < n; ++i) arguments.emplace_back(mgp::list_at(args, i));
  return arguments;
}

void InsertMessage(mgp::RecordFactory &factory, const char *msg) {
  auto record = factory.NewRecord();
  record.Insert(kResultMessage, msg);
}

void SetStreamwalkUpdater(mgp_list *args, mgp_graph * /*graph*/, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  try {
    CheckEnterprise();
    auto arguments = CollectArgs(args);
    mgp::RecordFactory factory(result);

    std::lock_guard<std::mutex> lock(g_mtx);
    if (g_ctx.IsInitialized()) {
      InsertMessage(factory,
                    "StreamWalk updater is already initialized. Call: `CALL node2vec_online.reset() YIELD *;` in "
                    "order to set parameters again. Warning: all embeddings will be lost.");
      return;
    }

    int64_t half_life = arguments[0].ValueInt();
    int64_t max_length = arguments[1].ValueInt();
    double beta = arguments[2].ValueNumeric();
    int64_t cutoff = arguments[3].ValueInt();
    int64_t sampled_walks = arguments[4].ValueInt();
    bool full_walks = arguments[5].ValueBool();
    if (half_life <= 0) {
      throw std::runtime_error("half_life must be positive.");
    }

    g_ctx.updater = std::make_unique<node2vec_alg::StreamWalkUpdater>(
        half_life, static_cast<int>(max_length), beta, cutoff, static_cast<int>(sampled_walks), full_walks);
    InsertMessage(factory, "Parameters for StreamWalk updater are set.");
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
  }
}

void SetWord2vecLearner(mgp_list *args, mgp_graph * /*graph*/, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  try {
    CheckEnterprise();
    auto arguments = CollectArgs(args);
    mgp::RecordFactory factory(result);

    std::lock_guard<std::mutex> lock(g_mtx);
    if (g_ctx.IsInitialized()) {
      InsertMessage(factory,
                    "Word2Vec learner is already initialized. Call: `CALL node2vec_online.reset() YIELD *;` in order "
                    "to set parameters again. Warning: all embeddings will be lost.");
      return;
    }

    int64_t embedding_dimension = arguments[0].ValueInt();
    double learning_rate = arguments[1].ValueNumeric();
    bool skip_gram = arguments[2].ValueBool();
    double negative_rate = arguments[3].ValueNumeric();
    int64_t threads = arguments[4].ValueInt();
    if (threads <= 0) {
      unsigned hw = std::thread::hardware_concurrency();
      threads = hw > 0 ? static_cast<int64_t>(hw) : 1;
    }

    node2vec_alg::Word2VecParams wp;
    wp.vector_size = static_cast<int>(embedding_dimension);
    wp.window = 1;
    wp.min_count = 1;
    wp.alpha = learning_rate;
    wp.min_alpha = learning_rate;
    wp.sg = skip_gram;
    wp.epochs = 1;
    wp.workers = static_cast<int>(threads);
    if (negative_rate <= 0) {
      wp.negative = 0;
      wp.hs = true;  // hierarchical softmax
    } else {
      // A positive but sub-1 negative_rate truncates to 0, which would disable
      // both negative sampling and HS — a silent no-op learner. Clamp to >= 1.
      wp.negative = std::max(1, static_cast<int>(negative_rate));
      wp.hs = false;
    }

    g_ctx.learner = std::make_unique<node2vec_alg::Word2Vec>(wp);
    InsertMessage(factory, "Parameters for Word2Vec learner are set.");
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
  }
}

void Reset(mgp_list * /*args*/, mgp_graph * /*graph*/, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  try {
    CheckEnterprise();
    std::lock_guard<std::mutex> lock(g_mtx);
    g_ctx.Reset();
    mgp::RecordFactory factory(result);
    InsertMessage(factory, "Updater and learner are ready to be set again.");
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
  }
}

void Get(mgp_list * /*args*/, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  try {
    CheckEnterprise();
    mgp::Graph graph(memgraph_graph);
    mgp::RecordFactory factory(result);

    std::lock_guard<std::mutex> lock(g_mtx);
    if (!g_ctx.IsInitialized()) {
      throw std::runtime_error(
          "Learner or updater are not initialized. Initialize them by calling: `CALL "
          "node2vec_online.set_word2vec_learner() YIELD *;` and `CALL node2vec_online.set_streamwalk_updater() YIELD "
          "*;`");
    }

    auto embeddings = g_ctx.learner->GetEmbeddings();
    // Emit rows in ascending node-id order; the embeddings map is unordered, so
    // otherwise the row order is arbitrary and shifts as the vocabulary rehashes
    // (mirrors the batch module's SortedNodeIds).
    std::vector<int64_t> ids;
    ids.reserve(embeddings.size());
    for (const auto &kv : embeddings) ids.push_back(kv.first);
    std::ranges::sort(ids);
    for (const int64_t id : ids) {
      const auto &vec = embeddings.at(id);
      mgp::List emb(vec.size());
      for (const float x : vec) emb.AppendExtend(mgp::Value(static_cast<double>(x)));
      auto record = factory.NewRecord();
      record.Insert(kResultNode, graph.GetNodeById(mgp::Id::FromInt(id)));
      record.Insert(kResultEmbedding, emb);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
  }
}

void Update(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  try {
    CheckEnterprise();
    auto arguments = CollectArgs(args);
    const mgp::Graph graph(memgraph_graph);

    std::lock_guard<std::mutex> lock(g_mtx);
    if (!g_ctx.IsInitialized()) {
      throw std::runtime_error(
          "Learner or updater are not initialized. Initialize them by calling: `CALL "
          "node2vec_online.set_word2vec_learner() YIELD *;` and `CALL node2vec_online.set_streamwalk_updater() YIELD "
          "*;`");
    }

    const auto current_time = static_cast<int64_t>(std::time(nullptr));
    auto edges = arguments[0].ValueList();
    for (size_t i = 0; i < edges.Size(); ++i) {
      graph.CheckMustAbort();  // stay responsive to query termination / timeout
      auto rel = edges[i].ValueRelationship();
      const int64_t source = rel.From().Id().AsInt();
      const int64_t target = rel.To().Id().AsInt();
      auto pairs = g_ctx.updater->ProcessNewEdge(source, target, current_time);
      g_ctx.learner->PartialFit(pairs);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
  }
}

void Help(mgp_list * /*args*/, mgp_graph * /*graph*/, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  try {
    CheckEnterprise();
    mgp::RecordFactory factory(result);

    // Emits a manual-page section: `title` appears in the name column of the
    // first line, subsequent lines have an empty name column.
    auto emit_section = [&](const char *title, std::initializer_list<const char *> body) {
      bool first = true;
      for (const char *line : body) {
        auto record = factory.NewRecord();
        record.Insert(kResultName, first ? title : "");
        // NOLINTNEXTLINE(readability-suspicious-call-argument): field name then value, not swapped.
        record.Insert(kResultValue, line);
        first = false;
      }
    };

    emit_section("Procedure 'help'", {"Shows manual page for node2vec_online"});

    emit_section("Procedure 'set_streamwalk_updater'",
                 {"This function sets the updater to StreamWalk.",
                  "",
                  "If parameters are already set, the function doesn't reset them.",
                  "",
                  ":param half_life: Half-life in seconds for time decay",
                  ":param max_length: Maximum length of the sampled temporal random walks",
                  ":param beta: Damping factor for long paths",
                  ":param cutoff: Temporal cutoff in seconds to exclude very distant past",
                  ":param sampled_walks: Number of sampled walks for each edge update",
                  ":param full_walks: Return every node of the sampled walk (full_walks=True) or",
                  "    only the endpoints of the walk (full_walks=False)",
                  "",
                  "Example call (give all information):",
                  "    CALL node2vec_online.set_streamwalk_updater(7200, 3, 0.9, 604800, 4, False) YIELD *;",
                  "",
                  "Example call (with predefined parameters):",
                  "    CALL node2vec_online.set_streamwalk_updater() YIELD *;"});

    emit_section("Procedure 'set_word2vec_learner'",
                 {"This function needs to be called to set the learner parameters.",
                  "",
                  "If parameters are already set, the function doesn't reset them.",
                  "",
                  ":param embedding_dimension: number of dimensions of the embedding vector",
                  ":param learning_rate: learning rate",
                  ":param skip_gram: use the skip-gram model",
                  ":param negative_rate: negative rate for the skip-gram model",
                  ":param threads: maximum number of threads for parallelization",
                  "",
                  "Example call (give all information):",
                  "    CALL node2vec_online.set_word2vec_learner(128, 0.01, True, 10, 1) YIELD *;",
                  "",
                  "Example call (with predefined parameters):",
                  "    CALL node2vec_online.set_word2vec_learner() YIELD *;"});

    emit_section("Procedure 'get'",
                 {"Returns the current node embeddings as (node, embedding) records.",
                  "",
                  "Example call:",
                  "    CALL node2vec_online.get() YIELD node, embedding RETURN node, embedding;"});

    emit_section("Procedure 'update'",
                 {"Updates node embeddings from newly created edges.",
                  "The learner and updater must be set first.",
                  "",
                  "Typically invoked from a trigger:",
                  "    CREATE TRIGGER update_embeddings ON --> CREATE BEFORE COMMIT",
                  "        EXECUTE CALL node2vec_online.update(createdEdges);",
                  "",
                  "It can also be called directly on a collection of edges:",
                  "    MATCH (n)-[e]->(m) WITH COLLECT(e) AS edges",
                  "    CALL node2vec_online.update(edges);"});

    emit_section("Procedure 'reset'",
                 {"Clears the updater, the learner and all learned embeddings, so the",
                  "parameters can be set again.",
                  "",
                  "Example call:",
                  "    CALL node2vec_online.reset() YIELD *;"});
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
  }
}

void AddOpt(mgp_proc *proc, const char *name, mgp_type *type, mgp_value *def) {
  mgp::proc_add_opt_arg(proc, name, type, def);
  mgp::value_destroy(def);
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard(memory);

    {
      auto *proc = mgp::module_add_read_procedure(module, "set_streamwalk_updater", SetStreamwalkUpdater);
      AddOpt(proc, "half_life", mgp::type_int(), mgp::value_make_int(7200, memory));
      AddOpt(proc, "max_length", mgp::type_int(), mgp::value_make_int(3, memory));
      AddOpt(proc, "beta", mgp::type_number(), mgp::value_make_double(0.9, memory));
      AddOpt(proc, "cutoff", mgp::type_int(), mgp::value_make_int(604800, memory));
      AddOpt(proc, "sampled_walks", mgp::type_int(), mgp::value_make_int(4, memory));
      AddOpt(proc, "full_walks", mgp::type_bool(), mgp::value_make_bool(0, memory));
      mgp::proc_add_result(proc, kResultMessage, mgp::type_string());
    }
    {
      auto *proc = mgp::module_add_read_procedure(module, "set_word2vec_learner", SetWord2vecLearner);
      AddOpt(proc, "embedding_dimension", mgp::type_int(), mgp::value_make_int(128, memory));
      AddOpt(proc, "learning_rate", mgp::type_number(), mgp::value_make_double(0.01, memory));
      AddOpt(proc, "skip_gram", mgp::type_bool(), mgp::value_make_bool(1, memory));
      AddOpt(proc, "negative_rate", mgp::type_number(), mgp::value_make_double(10.0, memory));
      AddOpt(proc, "threads", mgp::type_int(), mgp::value_make_int(1, memory));
      mgp::proc_add_result(proc, kResultMessage, mgp::type_string());
    }
    {
      auto *proc = mgp::module_add_read_procedure(module, "reset", Reset);
      mgp::proc_add_result(proc, kResultMessage, mgp::type_string());
    }
    {
      auto *proc = mgp::module_add_read_procedure(module, "get", Get);
      mgp::proc_add_result(proc, kResultNode, mgp::type_node());
      mgp::proc_add_result(proc, kResultEmbedding, mgp::type_list(mgp::type_float()));
    }
    {
      auto *proc = mgp::module_add_read_procedure(module, "update", Update);
      mgp::proc_add_arg(proc, "edges", mgp::type_list(mgp::type_relationship()));
    }
    {
      auto *proc = mgp::module_add_read_procedure(module, "help", Help);
      mgp::proc_add_result(proc, kResultName, mgp::type_string());
      mgp::proc_add_result(proc, kResultValue, mgp::type_string());
    }
  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
