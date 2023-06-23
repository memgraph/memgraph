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

#include <algorithm>
#include <iterator>
#ifdef MG_ENTERPRISE

#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "global.hpp"
#include "storage/v2/storage.hpp"
#include "utils/result.hpp"

#include "handler.hpp"

namespace memgraph::dbms {

class StorageHandler {
 public:
  using HandlerT = Handler<storage::Storage, storage::Config>;

  HandlerT::NewResult New(const std::string &name, const storage::Config &config) {
    // Control that no one is using the same data directory
    if (std::any_of(handler_.cbegin(), handler_.cend(), [&](const auto &elem) {
          return elem.second.config().durability.storage_directory == config.durability.storage_directory;
        })) {
      // LOG
      return NewError::EXISTS;
    }
    return handler_.New(name, std::forward_as_tuple(config), std::forward_as_tuple(config, name));
  }

  auto Get(const std::string &name) { return handler_.Get(name); }

  auto GetConfig(const std::string &name) const { return handler_.GetConfig(name); }

  auto Delete(const std::string &name) { return handler_.Delete(name); }

  auto Has(const std::string &name) const { return handler_.Has(name); }

  std::vector<std::string> All() const {
    std::vector<std::string> res;
    res.reserve(std::distance(handler_.cbegin(), handler_.cend()));
    std::for_each(handler_.cbegin(), handler_.cend(), [&](const auto &elem) { res.push_back(elem.first); });
    return res;
  }

 private:
  HandlerT handler_;
};

}  // namespace memgraph::dbms

#endif
