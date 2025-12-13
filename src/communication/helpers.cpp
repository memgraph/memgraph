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

#include <array>

#include <openssl/err.h>

#include "communication/helpers.hpp"

namespace memgraph::communication {

std::string SslGetLastError() {
  std::array<char, 256> buff;  // OpenSSL docs specify min 120 bytes
  auto err = ERR_get_error();
  ERR_error_string_n(err, buff.data(), buff.size());
  return {buff.data()};
}
}  // namespace memgraph::communication
