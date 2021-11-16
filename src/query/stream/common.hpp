// Copyright 2021 Memgraph Ltd.
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

#include <chrono>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include <json/json.hpp>

#include "query/procedure/mg_procedure_impl.hpp"

namespace query {

template <typename TMessage>
using ConsumerFunction = std::function<void(const std::vector<TMessage> &)>;

struct CommonStreamInfo {
  std::optional<std::chrono::milliseconds> batch_interval;
  std::optional<int64_t> batch_size;
  std::string transformation_name;
};

template <typename T>
concept ConvertableToJson = requires(T value, nlohmann::json data) {
  { to_json(data, std::move(value)) } -> std::same_as<void>;
  { from_json(data, value) } -> std::same_as<void>;
};

template <typename T>
concept ConvertableToMgpMessage = requires(T value) {
  mgp_message{value};
};

template <typename TStream>
concept Stream = requires(TStream stream) {
  typename TStream::StreamInfo;
  typename TStream::Message;
  TStream{std::string{""}, typename TStream::StreamInfo{}, ConsumerFunction<typename TStream::Message>{}};
  { stream.Start() } -> std::same_as<void>;
  { stream.Stop() } -> std::same_as<void>;
  { stream.IsRunning() } -> std::same_as<bool>;
  {
    stream.Check(std::optional<std::chrono::milliseconds>{}, std::optional<int64_t>{},
                 ConsumerFunction<typename TStream::Message>{})
    } -> std::same_as<void>;
  { typename TStream::StreamInfo{}.common_info } -> std::same_as<CommonStreamInfo>;

  requires ConvertableToMgpMessage<typename TStream::Message>;
  requires ConvertableToJson<typename TStream::StreamInfo>;
};

enum class StreamSourceType : uint8_t { KAFKA };

template <Stream T>
StreamSourceType StreamType(const T & /*stream*/);

const std::string kCommonInfoKey = "common_info";

void to_json(nlohmann::json &data, CommonStreamInfo &&info);
void from_json(const nlohmann::json &data, CommonStreamInfo &common_info);
}  // namespace query
