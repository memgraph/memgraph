// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cstdint>
#include <optional>
#include <sstream>

#include "csv/parsing.hpp"
#include "utils/string.hpp"

namespace mg = memgraph;
namespace csv = mg::csv;

using pmr_str = mg::utils::pmr::string;

extern "C" int LLVMFuzzerTestOneInput(std::uint8_t const *data, std::size_t size) {
  // need to parse a config
  if (size < 4) return 0;
  auto const with_header = bool(data[0]);
  auto const ignore_bad = bool(data[1]);
  auto const delim_size = data[2];
  auto const quote_size = data[3];
  // 0 will be nullopt, everything else will be one smaller
  // 0      , 1, 2, 3, 4
  // nullopt, 0, 1, 2, 3
  auto const delim_real_size = delim_size == 0 ? 0 : delim_size - 1;
  auto const quote_real_size = quote_size == 0 ? 0 : quote_size - 1;
  // not worth testing if too large
  if (delim_real_size > 3 || quote_real_size > 3) return 0;

  // is there enough space for them to exist
  if (size < 4 + delim_real_size + quote_real_size) return 0;
  auto const delim_start = 4;
  auto const delim_end = delim_start + delim_real_size;
  auto const quote_start = delim_end;
  auto const quote_end = quote_start + quote_real_size;

  auto *mem = mg::utils::NewDeleteResource();
  auto delim = delim_size == 0 ? std::optional<pmr_str>{} : pmr_str(&data[delim_start], &data[delim_end], mem);
  auto quote = quote_size == 0 ? std::optional<pmr_str>{} : pmr_str(&data[quote_start], &data[quote_end], mem);

  auto const remaining = static_cast<int64_t>(size) - quote_end;
  if (remaining < 0) __builtin_trap();  // if this hits, above parsing is wrong

  // #############################################################################################################

  // build Config
  auto cfg = csv::Reader::Config{with_header, ignore_bad, std::move(delim), std::move(quote)};

  // build CSV source
  auto ss = std::stringstream{};
  ss.write(reinterpret_cast<char const *>(&data[quote_end]), static_cast<std::streamsize>(remaining));
  auto source = csv::StreamCsvSource{std::move(ss)};

  // #############################################################################################################
  try {
    auto reader = memgraph::csv::Reader(std::move(source), std::move(cfg));
    auto const header = reader.GetHeader();
    asm volatile("" : : "g"(header) : "memory");
    while (true) {
      auto row = reader.GetNextRow(mem);
      if (!row) break;
      asm volatile("" : : "g"(row) : "memory");
    }
  } catch (csv::CsvReadException const &) {
    // CsvReadException is ok
  }

  return 0;
}
