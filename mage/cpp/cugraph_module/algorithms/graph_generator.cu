// Copyright (c) 2016-2022 Memgraph Ltd. [https://memgraph.com]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mg_cugraph_utility.hpp"

namespace {
using vertex_t = int64_t;
using edge_t = int64_t;

constexpr char const *kProcedureRmat = "rmat";

constexpr char const *kArgumentScale = "scale";
constexpr char const *kArgumentNumEdges = "num_edges";
constexpr char const *kArgumentA = "a";
constexpr char const *kArgumentB = "b";
constexpr char const *kArgumentC = "c";
constexpr char const *kArgumentSeed = "seed";
constexpr char const *kArgumentClipAndFlip = "clip_and_flip";

constexpr char const *kResultFieldSource = "source";
constexpr char const *kResultFieldTarget = "target";

void RmatProc(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto scale = static_cast<std::size_t>(mgp::value_get_int(mgp::list_at(args, 0)));
    auto num_edges = static_cast<std::size_t>(mgp::value_get_int(mgp::list_at(args, 1)));
    auto a = mgp::value_get_double(mgp::list_at(args, 2));
    auto b = mgp::value_get_double(mgp::list_at(args, 3));
    auto c = mgp::value_get_double(mgp::list_at(args, 4));
    auto seed = static_cast<std::uint64_t>(mgp::value_get_int(mgp::list_at(args, 5)));
    auto clip_and_flip = mgp::value_get_bool(mgp::list_at(args, 6));

    // Define handle
    raft::handle_t handle{};

    // Create RNG state from seed for cuGraph 25.x API
    raft::random::RngState rng_state(seed);

    // Generate RMAT edges using cuGraph 25.x API
    auto edges = mg_cugraph::GenerateCugraphRMAT<vertex_t>(
        rng_state, scale, num_edges, a, b, c, clip_and_flip, handle);

    // Output results
    for (const auto &[src, dst] : edges) {
      auto *record = mgp::result_new_record(result);
      if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

      mg_utility::InsertIntValueResult(record, kResultFieldSource, static_cast<int64_t>(src), memory);
      mg_utility::InsertIntValueResult(record, kResultFieldTarget, static_cast<int64_t>(dst), memory);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_scale;
  mgp_value *default_num_edges;
  mgp_value *default_a;
  mgp_value *default_b;
  mgp_value *default_c;
  mgp_value *default_seed;
  mgp_value *default_clip_and_flip;
  try {
    auto *rmat_proc = mgp::module_add_read_procedure(module, kProcedureRmat, RmatProc);

    default_scale = mgp::value_make_int(4, memory);
    default_num_edges = mgp::value_make_int(100, memory);
    default_a = mgp::value_make_double(0.57, memory);
    default_b = mgp::value_make_double(0.19, memory);
    default_c = mgp::value_make_double(0.19, memory);
    default_seed = mgp::value_make_int(42, memory);
    default_clip_and_flip = mgp::value_make_bool(false, memory);

    mgp::proc_add_opt_arg(rmat_proc, kArgumentScale, mgp::type_int(), default_scale);
    mgp::proc_add_opt_arg(rmat_proc, kArgumentNumEdges, mgp::type_int(), default_num_edges);
    mgp::proc_add_opt_arg(rmat_proc, kArgumentA, mgp::type_float(), default_a);
    mgp::proc_add_opt_arg(rmat_proc, kArgumentB, mgp::type_float(), default_b);
    mgp::proc_add_opt_arg(rmat_proc, kArgumentC, mgp::type_float(), default_c);
    mgp::proc_add_opt_arg(rmat_proc, kArgumentSeed, mgp::type_int(), default_seed);
    mgp::proc_add_opt_arg(rmat_proc, kArgumentClipAndFlip, mgp::type_bool(), default_clip_and_flip);

    mgp::proc_add_result(rmat_proc, kResultFieldSource, mgp::type_int());
    mgp::proc_add_result(rmat_proc, kResultFieldTarget, mgp::type_int());
  } catch (const std::exception &e) {
    mgp_value_destroy(default_scale);
    mgp_value_destroy(default_num_edges);
    mgp_value_destroy(default_a);
    mgp_value_destroy(default_b);
    mgp_value_destroy(default_c);
    mgp_value_destroy(default_seed);
    mgp_value_destroy(default_clip_and_flip);
    return 1;
  }

  mgp_value_destroy(default_scale);
  mgp_value_destroy(default_num_edges);
  mgp_value_destroy(default_a);
  mgp_value_destroy(default_b);
  mgp_value_destroy(default_c);
  mgp_value_destroy(default_seed);
  mgp_value_destroy(default_clip_and_flip);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
