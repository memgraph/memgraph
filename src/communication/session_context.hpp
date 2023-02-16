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

#include <cstdint>
#include <memory>
#include "communication/context.hpp"

class BoltSession;

namespace memgraph::communication {

class BoltSessionContext : public memgraph::communication::SessionContext {
 public:
  ~BoltSessionContext(){};
  BoltSessionContext(const BoltSessionContext &) = delete;
  BoltSessionContext &operator=(const BoltSessionContext &) = delete;
  BoltSessionContext &operator=(BoltSessionContext &&other) = delete;
  BoltSessionContext(BoltSessionContext &&other) noexcept;

  explicit BoltSessionContext(std::shared_ptr<BoltSession> bolt_session) : bolt_session(bolt_session) {}

  uint64_t GetTransactionId() override;

  void Terminate() override {}

 private:
  std::shared_ptr<BoltSession> bolt_session;
};

}  // namespace memgraph::communication
