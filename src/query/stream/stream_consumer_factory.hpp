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

#include "query/interpreter_context.hpp"
#include "query/stream/istream_consumer_factory.hpp"

namespace memgraph::query::stream {
struct StreamConsumerFactory : IStreamConsumerFactory {
  explicit StreamConsumerFactory(memgraph::query::InterpreterContext *interpreterContext)
      : interpreter_context(interpreterContext) {}

  auto make_consumer(utils::Tag<KafkaStream> tag, dbms::DatabaseAccess db_acc, const std::string &stream_name,
                     const std::string &transformation_name, std::optional<std::string> owner)
      -> Consumer<KafkaStream> override;

  auto make_consumer(utils::Tag<PulsarStream> tag, dbms::DatabaseAccess db_acc, const std::string &stream_name,
                     const std::string &transformation_name, std::optional<std::string> owner)
      -> Consumer<PulsarStream> override;

  auto make_check_consumer(utils::Tag<KafkaStream> tag, dbms::DatabaseAccess db_acc, const std::string &stream_name,
                           const std::string &transformation_name, TransformationResult &test_result)
      -> Consumer<KafkaStream> override;

  auto make_check_consumer(utils::Tag<PulsarStream> tag, dbms::DatabaseAccess db_acc, const std::string &stream_name,
                           const std::string &transformation_name, TransformationResult &test_result)
      -> Consumer<PulsarStream> override;

 private:
  ::memgraph::query::InterpreterContext *interpreter_context;
};
}  // namespace memgraph::query::stream
