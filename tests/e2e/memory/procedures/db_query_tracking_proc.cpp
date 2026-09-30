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

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <exception>
#include <string>
#include <string_view>
#include <thread>

#include "mg_procedure.h"
#include "mgp.hpp"
#include "utils/on_scope_exit.hpp"

namespace {

constexpr std::string_view kSignalLabel = "DbQueryMemorySignal";
constexpr std::string_view kSignalProperty = "signal_id";

bool IsValidSignalId(std::string_view signal_id) {
  return !signal_id.empty() && std::all_of(signal_id.begin(), signal_id.end(), [](unsigned char ch) {
    return std::isalnum(ch) || ch == '_' || ch == '-';
  });
}

bool HasReleaseSignal(mgp_graph *graph_ptr, std::string_view signal_id) {
  auto graph = mgp::Graph(graph_ptr);
  for (const auto &node : graph.Nodes()) {
    if (!node.HasLabel(std::string{kSignalLabel})) {
      continue;
    }
    auto value = node.GetProperty(std::string{kSignalProperty});
    if (!value.IsString()) {
      continue;
    }
    if (value.ValueString() == signal_id) {
      return true;
    }
  }
  return false;
}

void HoldQueryMemory(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto signal_id = arguments[0].ValueString();
    auto mebibytes = arguments[1].ValueInt();
    auto max_wait_ms = arguments[2].ValueInt();
    if (!IsValidSignalId(signal_id)) {
      record_factory.SetErrorMessage("signal_id must be non-empty and contain only [A-Za-z0-9_-]");
      return;
    }
    if (mebibytes <= 0) {
      record_factory.SetErrorMessage("mebibytes must be positive");
      return;
    }
    if (max_wait_ms < 0) {
      record_factory.SetErrorMessage("max_wait_ms must be non-negative");
      return;
    }

    void *ptr{nullptr};
    const auto bytes = static_cast<std::size_t>(mebibytes) * 1024ULL * 1024ULL;
    const auto alloc_err = mgp_alloc(memory, bytes, &ptr);
    if (alloc_err != mgp_error::MGP_ERROR_NO_ERROR || ptr == nullptr) {
      record_factory.SetErrorMessage("failed to allocate query memory");
      return;
    }

    auto cleanup = memgraph::utils::OnScopeExit{[&memory, &ptr]() {
      if (ptr != nullptr) {
        mgp_free(memory, ptr);
      }
    }};

    std::memset(ptr, 0xAB, bytes);

    auto waited_ms = int64_t{0};
    while (!HasReleaseSignal(graph, signal_id) && waited_ms < max_wait_ms) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      waited_ms += 10;
    }

    if (!HasReleaseSignal(graph, signal_id)) {
      record_factory.SetErrorMessage("timed out waiting for release signal");
      return;
    }

    auto record = record_factory.NewRecord();
    record.Insert("allocated_bytes", static_cast<int64_t>(bytes));
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void ReleaseHoldQueryMemory(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto signal_id = arguments[0].ValueString();
    if (!IsValidSignalId(signal_id)) {
      record_factory.SetErrorMessage("signal_id must be non-empty and contain only [A-Za-z0-9_-]");
      return;
    }
    auto signal_node = mgp::Graph(graph).CreateNode();
    signal_node.AddLabel(kSignalLabel);
    signal_node.SetProperty(std::string{kSignalProperty}, mgp::Value(std::string{signal_id}));
    auto record = record_factory.NewRecord();
    record.Insert("released", true);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    AddProcedure(HoldQueryMemory,
                 "hold_query_memory",
                 mgp::ProcedureType::Read,
                 {mgp::Parameter("signal_id", mgp::Type::String),
                  mgp::Parameter("mebibytes", mgp::Type::Int),
                  mgp::Parameter("max_wait_ms", mgp::Type::Int)},
                 {mgp::Return("allocated_bytes", mgp::Type::Int)},
                 module,
                 memory);
    AddProcedure(ReleaseHoldQueryMemory,
                 "release_hold_query_memory",
                 mgp::ProcedureType::Write,
                 {mgp::Parameter("signal_id", mgp::Type::String)},
                 {mgp::Return("released", mgp::Type::Bool)},
                 module,
                 memory);
  } catch (const std::exception &) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
