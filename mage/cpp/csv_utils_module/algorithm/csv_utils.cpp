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

#include "csv_utils.hpp"

namespace CsvUtils {

// NOLINTNEXTLINE(misc-unused-parameters)
void CreateCsvFile(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto filepath = arguments[0].ValueString();
    const auto content = arguments[1].ValueString();
    const auto is_append = arguments[2].ValueBool();

    std::ofstream fout;
    fout.open(std::string(filepath), is_append ? std::ofstream::app : std::ofstream::out);
    fout << content << std::flush;
    fout.close();

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kArgumentCreateCsvFile1).c_str(), filepath);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
  return;
}

// NOLINTNEXTLINE(misc-unused-parameters)
void DeleteCsvFile(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const std::string_view filepath = arguments[0].ValueString();
    std::remove(std::string(filepath).c_str());

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
  return;
}

}  // namespace CsvUtils
