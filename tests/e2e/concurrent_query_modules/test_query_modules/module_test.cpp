// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <mgp.hpp>

#include <chrono>
#include <cmath>
#include <list>
#include <thread>

constexpr char const *kProcedureHackerNews = "hacker_news";
constexpr char const *kArgumentHackerNewsVotes = "votes";
constexpr char const *kArgumentHackerNewsItemHourAge = "item_hour_age";
constexpr char const *kArgumentHackerNewsGravity = "gravity";
constexpr char const *kReturnHackerNewsScore = "score";

void HackerNews(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  const auto &arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto votes = arguments[0].ValueInt();
    const auto item_hour_age = arguments[1].ValueInt();
    const auto gravity = arguments[2].ValueDouble();
    const auto score = 1000000.0 * (votes / pow((item_hour_age + 2), gravity));
    auto record = record_factory.NewRecord();
    record.Insert(kReturnHackerNewsScore, score);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);
    std::vector<mgp::Parameter> params = {
        mgp::Parameter(kArgumentHackerNewsVotes, mgp::Type::Int),
        mgp::Parameter(kArgumentHackerNewsItemHourAge, mgp::Type::Int),
        mgp::Parameter(kArgumentHackerNewsGravity, mgp::Type::Double),
    };
    std::vector<mgp::Return> returns = {mgp::Return(kReturnHackerNewsScore, mgp::Type::Double)};
    AddProcedure(HackerNews, kProcedureHackerNews, mgp::ProcedureType::Read, params, returns, module, memory);
  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
