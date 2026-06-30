// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// Batch node2vec query module. Computes node embeddings by sampling biased
// second-order random walks and training a Word2Vec model over them. This is a
// C++ reimplementation of the previous Python module (mage/python/node2vec.py),
// removing the dependency on `gensim`. Procedure names, parameters and result
// signatures are unchanged.

#include <mgp.hpp>

#include <string>
#include <unordered_map>
#include <vector>

#include "algorithm/second_order_random_walk.hpp"
#include "algorithm/word2vec.hpp"

namespace {

constexpr char kProcGetEmbeddings[] = "get_embeddings";
constexpr char kProcSetEmbeddings[] = "set_embeddings";
constexpr char kProcHelp[] = "help";

constexpr char kResultNodes[] = "nodes";
constexpr char kResultEmbeddings[] = "embeddings";
constexpr char kResultName[] = "name";
constexpr char kResultValue[] = "value";

constexpr char kEmbeddingProperty[] = "embedding";

// Parsed node2vec parameters (in registration order). Defaults mirror the
// previous Python signature.
struct Params {
  bool is_directed = false;
  double p = 2.0;
  double q = 0.5;
  int64_t num_walks = 4;
  int64_t walk_length = 5;
  int64_t vector_size = 100;
  double alpha = 0.025;
  int64_t window = 5;
  int64_t min_count = 1;
  int64_t seed = 1;
  int64_t workers = 1;
  double min_alpha = 0.0001;
  int64_t sg = 1;
  int64_t hs = 0;
  int64_t negative = 5;
  int64_t epochs = 5;
  std::string edge_weight_property = "weight";
};

Params ParseParams(const std::vector<mgp::Value> &args) {
  Params prm;
  prm.is_directed = args[0].ValueBool();
  prm.p = args[1].ValueNumeric();
  prm.q = args[2].ValueNumeric();
  prm.num_walks = args[3].ValueInt();
  prm.walk_length = args[4].ValueInt();
  prm.vector_size = args[5].ValueInt();
  prm.alpha = args[6].ValueNumeric();
  prm.window = args[7].ValueInt();
  prm.min_count = args[8].ValueInt();
  prm.seed = args[9].ValueInt();
  prm.workers = args[10].ValueInt();
  prm.min_alpha = args[11].ValueNumeric();
  prm.sg = args[12].ValueInt();
  prm.hs = args[13].ValueInt();
  prm.negative = args[14].ValueInt();
  prm.epochs = args[15].ValueInt();
  prm.edge_weight_property = std::string(args[16].ValueString());
  return prm;
}

// Builds the weighted node2vec graph from the Memgraph graph view.
node2vec_alg::N2vGraph BuildGraph(const mgp::Graph &graph, const Params &prm) {
  node2vec_alg::N2vGraph n2v(prm.is_directed);
  for (const auto node : graph.Nodes()) {
    int64_t from = node.Id().AsInt();
    for (const auto rel : node.OutRelationships()) {
      int64_t to = rel.To().Id().AsInt();
      double weight = 1.0;
      auto wval = rel.GetProperty(prm.edge_weight_property);
      if (wval.IsNumeric()) weight = wval.ValueNumeric();
      n2v.AddEdge(from, to, weight);
    }
  }
  n2v.Build();
  return n2v;
}

std::unordered_map<int64_t, std::vector<float>> ComputeEmbeddings(node2vec_alg::N2vGraph &n2v, const Params &prm) {
  node2vec_alg::SecondOrderRandomWalk walk(prm.p,
                                           prm.q,
                                           static_cast<int>(prm.num_walks),
                                           static_cast<int>(prm.walk_length),
                                           static_cast<uint64_t>(prm.seed));
  auto walks = walk.SampleNodeWalks(n2v);

  node2vec_alg::Word2VecParams wp;
  wp.vector_size = static_cast<int>(prm.vector_size);
  wp.window = static_cast<int>(prm.window);
  wp.min_count = static_cast<int>(prm.min_count);
  wp.workers = static_cast<int>(prm.workers);
  wp.alpha = prm.alpha;
  wp.min_alpha = prm.min_alpha;
  wp.seed = static_cast<int>(prm.seed);
  wp.epochs = static_cast<int>(prm.epochs);
  wp.sg = prm.sg != 0;
  wp.hs = prm.hs != 0;
  wp.negative = static_cast<int>(prm.negative);

  node2vec_alg::Word2Vec model(wp);
  model.Train(walks);
  return model.GetEmbeddings();
}

mgp::List EmbeddingToList(const std::vector<float> &vec) {
  mgp::List inner(vec.size());
  for (float x : vec) inner.AppendExtend(mgp::Value(static_cast<double>(x)));
  return inner;
}

std::vector<mgp::Value> CollectArgs(mgp_list *args) {
  std::vector<mgp::Value> arguments;
  const size_t n = mgp::list_size(args);
  arguments.reserve(n);
  for (size_t i = 0; i < n; ++i) arguments.emplace_back(mgp::list_at(args, i));
  return arguments;
}

void GetEmbeddings(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  try {
    auto arguments = CollectArgs(args);
    Params prm = ParseParams(arguments);
    mgp::Graph graph(memgraph_graph);

    auto n2v = BuildGraph(graph, prm);
    auto embeddings = ComputeEmbeddings(n2v, prm);

    mgp::List nodes_list(embeddings.size());
    mgp::List emb_list(embeddings.size());
    for (const auto &kv : embeddings) {
      nodes_list.AppendExtend(mgp::Value(graph.GetNodeById(mgp::Id::FromInt(kv.first))));
      emb_list.AppendExtend(mgp::Value(EmbeddingToList(kv.second)));
    }

    auto record = mgp::RecordFactory(result).NewRecord();
    record.Insert(kResultNodes, nodes_list);
    record.Insert(kResultEmbeddings, emb_list);
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
  }
}

void SetEmbeddings(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  try {
    auto arguments = CollectArgs(args);
    Params prm = ParseParams(arguments);
    mgp::Graph graph(memgraph_graph);

    auto n2v = BuildGraph(graph, prm);
    auto embeddings = ComputeEmbeddings(n2v, prm);

    mgp::List nodes_list(embeddings.size());
    mgp::List emb_list(embeddings.size());
    for (const auto &kv : embeddings) {
      auto node = graph.GetNodeById(mgp::Id::FromInt(kv.first));
      auto emb = EmbeddingToList(kv.second);
      node.SetProperty(kEmbeddingProperty, mgp::Value(emb));
      nodes_list.AppendExtend(mgp::Value(node));
      emb_list.AppendExtend(mgp::Value(std::move(emb)));
    }

    auto record = mgp::RecordFactory(result).NewRecord();
    record.Insert(kResultNodes, nodes_list);
    record.Insert(kResultEmbeddings, emb_list);
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
  }
}

void Help(mgp_list * /*args*/, mgp_graph * /*memgraph_graph*/, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  try {
    mgp::RecordFactory factory(result);
    const std::pair<const char *, const char *> lines[] = {
        {"node2vec.get_embeddings", "Computes node embeddings and returns (nodes, embeddings)."},
        {"node2vec.set_embeddings", "Computes node embeddings and writes them to the 'embedding' property."},
        {"node2vec.help", "Shows this help page."},
    };
    for (const auto &line : lines) {
      auto record = factory.NewRecord();
      record.Insert(kResultName, line.first);
      record.Insert(kResultValue, line.second);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
  }
}

// Registers the node2vec parameters (identical for get_embeddings and
// set_embeddings) as optional arguments on the given procedure.
void AddNode2vecArgs(mgp_proc *proc, mgp_memory *memory) {
  auto add_opt = [&](const char *name, mgp_type *type, mgp_value *def) {
    mgp::proc_add_opt_arg(proc, name, type, def);
    mgp::value_destroy(def);
  };
  add_opt("is_directed", mgp::type_bool(), mgp::value_make_bool(0, memory));
  add_opt("p", mgp::type_number(), mgp::value_make_double(2.0, memory));
  add_opt("q", mgp::type_number(), mgp::value_make_double(0.5, memory));
  add_opt("num_walks", mgp::type_int(), mgp::value_make_int(4, memory));
  add_opt("walk_length", mgp::type_int(), mgp::value_make_int(5, memory));
  add_opt("vector_size", mgp::type_int(), mgp::value_make_int(100, memory));
  add_opt("alpha", mgp::type_number(), mgp::value_make_double(0.025, memory));
  add_opt("window", mgp::type_int(), mgp::value_make_int(5, memory));
  add_opt("min_count", mgp::type_int(), mgp::value_make_int(1, memory));
  add_opt("seed", mgp::type_int(), mgp::value_make_int(1, memory));
  add_opt("workers", mgp::type_int(), mgp::value_make_int(1, memory));
  add_opt("min_alpha", mgp::type_number(), mgp::value_make_double(0.0001, memory));
  add_opt("sg", mgp::type_int(), mgp::value_make_int(1, memory));
  add_opt("hs", mgp::type_int(), mgp::value_make_int(0, memory));
  add_opt("negative", mgp::type_int(), mgp::value_make_int(5, memory));
  add_opt("epochs", mgp::type_int(), mgp::value_make_int(5, memory));
  add_opt("edge_weight_property", mgp::type_string(), mgp::value_make_string("weight", memory));
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    auto *get_proc = mgp::module_add_read_procedure(module, kProcGetEmbeddings, GetEmbeddings);
    AddNode2vecArgs(get_proc, memory);
    mgp::proc_add_result(get_proc, kResultNodes, mgp::type_list(mgp::type_node()));
    mgp::proc_add_result(get_proc, kResultEmbeddings, mgp::type_list(mgp::type_list(mgp::type_float())));

    auto *set_proc = mgp::module_add_write_procedure(module, kProcSetEmbeddings, SetEmbeddings);
    AddNode2vecArgs(set_proc, memory);
    mgp::proc_add_result(set_proc, kResultNodes, mgp::type_list(mgp::type_node()));
    mgp::proc_add_result(set_proc, kResultEmbeddings, mgp::type_list(mgp::type_list(mgp::type_float())));

    auto *help_proc = mgp::module_add_read_procedure(module, kProcHelp, Help);
    mgp::proc_add_result(help_proc, kResultName, mgp::type_string());
    mgp::proc_add_result(help_proc, kResultValue, mgp::type_string());
  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
