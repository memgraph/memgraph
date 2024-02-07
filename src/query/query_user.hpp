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

#include <string>
#include <vector>

#include "query/frontend/ast/ast.hpp"

namespace memgraph::query {

struct QueryUser {
  QueryUser(std::optional<std::string> name) : name_{std::move(name)} {}
  virtual ~QueryUser() = default;

  virtual bool IsAuthorized(const std::vector<AuthQuery::Privilege> &privileges, const std::string &db_name) const = 0;

#ifdef MG_ENTERPRISE
  virtual std::string GetDefaultDB() const = 0;
#endif

  std::string name() const { return name_ ? *name_ : ""; }

  bool operator==(const QueryUser &other) const = default;
  operator bool() const { return name_.has_value(); }

 private:
  std::optional<std::string> name_;
};

}  // namespace memgraph::query
