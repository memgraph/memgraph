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

#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <filesystem>
#include <fstream>
#include <mgp.hpp>
namespace fs = std::filesystem;

namespace CsvUtils {

/* create_csv_file constants */
constexpr std::string_view kProcedureCreateCsvFile = "create_csv_file";
constexpr std::string_view kArgumentCreateCsvFile1 = "filepath";
constexpr std::string_view kArgumentCreateCsvFile2 = "content";
constexpr std::string_view kArgumentCreateCsvFile3 = "is_append";

void CreateCsvFile(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

/* delete_csv_file constants */
constexpr std::string_view kProcedureDeleteCsvFile = "delete_csv_file";
constexpr std::string_view kArgumentDeleteCsvFile1 = "filepath";

void DeleteCsvFile(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

}  // namespace CsvUtils
