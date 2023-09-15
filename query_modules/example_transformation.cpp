// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <exception>
#include <mgp.hpp>

static constexpr std::string_view kQuery = "query";
static constexpr std::string_view kParameters = "parameters";

std::string EscapeString(std::string s) {
  std::string sign = "'";
  std::string replace_sign;

  size_t pos;
  while ((pos = s.find(sign)) != std::string::npos) {
    s.replace(pos, 1, replace_sign);
  }

  return s;
}

void Transformation(struct mgp_messages *messages, mgp_trans_context *ctx, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  auto record_factory = mgp::RecordFactory(result);
  try {
    auto stream_messages = mgp::Messages(messages);

    for (const mgp::Message &message : stream_messages) {
      auto record = record_factory.NewRecord();

      auto payload = EscapeString(message.Payload());

      auto query = "CREATE (:Data {payload: '" + payload + "'});";
      auto query_value = mgp::Value(query.data());

      record.Insert(kQuery.data(), query_value);
      record.Insert(kParameters.data(), mgp::Value());
    }
  } catch (std::exception &ex) {
    record_factory.SetErrorMessage(ex.what());
    return;
  }
}

extern "C" int mgp_init_module(mgp_module *module, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);
    mgp::AddTransformation(Transformation, "transform", module);
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
