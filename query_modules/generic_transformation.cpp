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

#include <fmt/format.h>
#include <exception>
#include <json/json.hpp>
#include <mgp.hpp>

static constexpr std::string_view kQuery = "query";
static constexpr std::string_view kParameters = "parameters";

void Transformation(struct mgp_messages *messages, mgp_trans_context *ctx, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  auto record_factory = mgp::RecordFactory(result);
  auto transformation_context = mgp::TransformationContext(ctx);
  try {
    auto stream_messages = mgp::Messages(messages);
    for (const mgp::Message &message : stream_messages) {
      auto record = record_factory.NewRecord();

      auto payload = nlohmann::json::parse(message.Payload());
      std::string generic_query = transformation_context.Query();
      for (auto &el : payload.items()) {
        auto key = fmt::format("${}", el.key());
        size_t pos;
        while ((pos = generic_query.find(key)) != std::string::npos) {
          generic_query.replace(pos, key.size(), el.value());
        }
      }

      auto query_value = mgp::Value(generic_query.data());

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
