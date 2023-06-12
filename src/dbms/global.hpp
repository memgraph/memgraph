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

#include <concepts>
#include <cstdint>
#include <string>

namespace memgraph::dbms {

class SessionInterface {
 public:
  virtual ~SessionInterface() = default;

  virtual std::string UUID() const = 0;
  virtual std::string GetDB() const = 0;
  virtual bool OnChange(const std::string &) = 0;
  virtual bool OnDelete(const std::string &) = 0;
};

enum class DeleteError : uint8_t {
  DEFAULT_DB,
  USING,
  NON_EXISTENT,
  FAIL,
};

enum class NewError : uint8_t {
  NO_CONFIGS,
  EXISTS,
  GENERIC,
};

}  // namespace memgraph::dbms
