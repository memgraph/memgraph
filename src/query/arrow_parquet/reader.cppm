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

module;

#include <functional>

#include "query/typed_value.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"

export module memgraph.query.arrow_parquet.reader;

import memgraph.utils.aws;

export namespace memgraph::query {

using Row = TypedValue::TMap;
using Header = utils::pmr::vector<TypedValue::TString>;

class ParquetReader {
 public:
  explicit ParquetReader(utils::pmr::string const &file, utils::S3Config s3_config, utils::MemoryResource *resource);
  ~ParquetReader();

  ParquetReader(ParquetReader const &other) = delete;
  ParquetReader &operator=(ParquetReader const &other) = delete;

  ParquetReader(ParquetReader &&other) = default;
  ParquetReader &operator=(ParquetReader &&other) = default;

  auto GetNextRow(Row &out) -> bool;

 private:
  struct impl;
  std::unique_ptr<impl> pimpl_;
};
}  // namespace memgraph::query
