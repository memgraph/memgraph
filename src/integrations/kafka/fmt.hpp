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

#pragma once

#if FMT_VERSION > 90000
#include <fmt/ostream.h>

#include <librdkafka/rdkafkacpp.h>

inline std::ostream &operator<<(std::ostream &os, const RdKafka::ErrorCode &code) {
  os << RdKafka::err2str(code);
  return os;
}
template <>
class fmt::formatter<RdKafka::ErrorCode> : public fmt::ostream_formatter {};
#endif
