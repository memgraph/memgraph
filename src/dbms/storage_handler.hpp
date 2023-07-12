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

#ifdef MG_ENTERPRISE

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "global.hpp"
#include "storage/v2/storage.hpp"
#include "utils/result.hpp"

#include "handler.hpp"

namespace memgraph::dbms {

/**
 * @brief Multi-database storage handler
 *
 */
class StorageHandler : public Handler<storage::Storage, storage::Config> {
 public:
  using HandlerT = Handler<storage::Storage, storage::Config>;

  /**
   * @brief Generate new storage associated with the passed name.
   *
   * @param name Name associating the new interpreter context
   * @param config Storage configuration
   * @return HandlerT::NewResult
   */
  HandlerT::NewResult New(const std::string &name, const storage::Config &config) {
    // Control that no one is using the same data directory
    if (std::any_of(cbegin(), cend(), [&](const auto &elem) {
          return elem.second.config().durability.storage_directory == config.durability.storage_directory;
        })) {
      spdlog::info("Tried to generate new storage using a claimed directory.");
      return NewError::EXISTS;
    }
    return HandlerT::New(name, std::forward_as_tuple(config), std::forward_as_tuple(config, name));
  }

  /**
   * @brief All currently active storage.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> All() const {
    std::vector<std::string> res;
    res.reserve(std::distance(cbegin(), cend()));
    std::for_each(cbegin(), cend(), [&](const auto &elem) { res.push_back(elem.first); });
    return res;
  }
};

}  // namespace memgraph::dbms

#endif
