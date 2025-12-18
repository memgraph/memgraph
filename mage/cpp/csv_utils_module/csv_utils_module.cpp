// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <mg_utils.hpp>
#include <mgp.hpp>

#include "algorithm/csv_utils.hpp"

extern "C" int mgp_init_module(mgp_module *module, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);
    mgp::AddProcedure(CsvUtils::CreateCsvFile, CsvUtils::kProcedureCreateCsvFile, mgp::ProcedureType::Read,
                      {
                          mgp::Parameter(CsvUtils::kArgumentCreateCsvFile1, mgp::Type::String),
                          mgp::Parameter(CsvUtils::kArgumentCreateCsvFile2, mgp::Type::String),
                          mgp::Parameter(CsvUtils::kArgumentCreateCsvFile3, mgp::Type::Bool, false),
                      },
                      {mgp::Return(CsvUtils::kArgumentDeleteCsvFile1, {mgp::Type::String})}, module, memory);
    mgp::AddProcedure(CsvUtils::DeleteCsvFile, CsvUtils::kProcedureDeleteCsvFile, mgp::ProcedureType::Read,
                      {
                          mgp::Parameter(CsvUtils::kArgumentDeleteCsvFile1, mgp::Type::String),
                      },
                      {}, module, memory);
  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
