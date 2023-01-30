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

#pragma once

#include <functional>

namespace memgraph::utils {

// TODO(tyler) ensure that Message continues to represent
// reasonable constraints around message types over time,
// as we adapt things to use Thrift-generated message types.
template <typename T>
concept Message = std::same_as<T, std::decay_t<T>>;

/// This is a concrete type that allows one message type to be
/// sent to a single address. Initially intended to be used by the
/// Shard to send messages to the local ShardManager.
template <Message M>
class Sender {
  std::function<void(M)> sender_;

 public:
  explicit Sender(std::function<void(M)> sender) : sender_(sender) {}

  void Send(M message) { sender_(message); }
};

}  // namespace memgraph::utils
