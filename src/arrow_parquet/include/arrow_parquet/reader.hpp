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

#include <filesystem>

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/io/api.h"
#include "arrow/io/file.h"
#include "parquet/arrow/reader.h"
#include "parquet/file_reader.h"
#include "parquet/properties.h"

#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"

// TODO (andi) Implement this for local and url files
namespace memgraph::arrow {

// Use PMR for efficient memory allocation
using Row = utils::pmr::vector<utils::pmr::string>;

class ParquetReader {
 public:
  explicit ParquetReader(std::string const &file, utils::MemoryResource *resource);

  // Destructor must be defined in .cpp file for pimpl idiom with unique_ptr
  ~ParquetReader();

  auto GetNextRow(utils::MemoryResource *resource) const -> std::optional<Row>;
  auto GetHeader(utils::MemoryResource *resource) const -> Row;

 private:
  std::shared_ptr<::arrow::Table> table_;
  // Faster compilation with mg-query is the reason
  struct impl;
  std::unique_ptr<impl> pimpl_;
};

}  // namespace memgraph::arrow
