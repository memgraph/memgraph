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

#include <cstddef>
#include <cstdint>
#include <string>

#include "query/frontend/opencypher/parser.hpp"

static constexpr size_t kMaxInputLen = 4096;

extern "C" int LLVMFuzzerTestOneInput(uint8_t const *data, size_t size) {
  if (size == 0 || size > kMaxInputLen) return 0;

  try {
    std::string const query(reinterpret_cast<char const *>(data), size);
    memgraph::query::frontend::opencypher::Parser parser(query);
  } catch (memgraph::query::SyntaxException const &) {
  } catch (std::exception const &) {
    // ANTLR runtime exceptions (e.g. IllegalArgumentException on invalid UTF-8)
  }

  return 0;
}
