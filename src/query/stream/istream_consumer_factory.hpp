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

#include <functional>
#include <vector>
#include "dbms/database.hpp"
#include "query/stream/sources.hpp"
#include "utils/tag.hpp"

namespace memgraph::query::stream {

using TransformationResult = std::vector<std::vector<TypedValue>>;

struct IStreamConsumerFactory {
  template <typename TStream>
  using Consumer = std::function<void(const std::vector<typename TStream::Message> &)>;

  virtual ~IStreamConsumerFactory() = default;

  virtual auto make_consumer(memgraph::utils::Tag<memgraph::query::stream::KafkaStream> /*tag*/,
                             memgraph::dbms::DatabaseAccess db_acc, const std::string &stream_name,
                             const std::string &transformation_name, std::optional<std::string> owner)
      -> Consumer<memgraph::query::stream::KafkaStream> = 0;

  virtual auto make_consumer(memgraph::utils::Tag<memgraph::query::stream::PulsarStream> /*tag*/,
                             memgraph::dbms::DatabaseAccess db_acc, const std::string &stream_name,
                             const std::string &transformation_name, std::optional<std::string> owner)
      -> Consumer<memgraph::query::stream::PulsarStream> = 0;

  virtual auto make_check_consumer(memgraph::utils::Tag<memgraph::query::stream::KafkaStream> tag,
                                   memgraph::dbms::DatabaseAccess db_acc, const std::string &stream_name,
                                   const std::string &transformation_name, TransformationResult &test_result)
      -> Consumer<memgraph::query::stream::KafkaStream> = 0;

  virtual auto make_check_consumer(memgraph::utils::Tag<memgraph::query::stream::PulsarStream> tag,
                                   memgraph::dbms::DatabaseAccess db_acc, const std::string &stream_name,
                                   const std::string &transformation_name, TransformationResult &test_result)
      -> Consumer<memgraph::query::stream::PulsarStream> = 0;
};
}  // namespace memgraph::query::stream
