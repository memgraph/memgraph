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

#include "storage/v2/config.hpp"

namespace memgraph::dbms {
struct SystemTransaction {
  struct Delta {
    enum class Action {
      CREATE_DATABASE,
    };

    static constexpr struct CreateDatabase {
    } create_database;

    Delta(CreateDatabase /*tag*/, storage::SalientConfig config)
        : action(Action::CREATE_DATABASE), config(std::move(config)) {}

    Delta(const Delta &) = delete;
    Delta(Delta &&) = delete;
    Delta &operator=(const Delta &) = delete;
    Delta &operator=(Delta &&) = delete;

    ~Delta() {
      switch (action) {
        case Action::CREATE_DATABASE:
          break;
          // Some deltas might have special destructor handling
      }
    }

    Action action;
    union {
      storage::SalientConfig config;
    };
  };

  explicit SystemTransaction(uint64_t timestamp) : system_timestamp(timestamp) {}

  // Currently system transitions support a single delta
  std::optional<Delta> delta{};
  uint64_t system_timestamp;
};

struct WAL {
  enum class Version : uint8_t {
    V0 = 0,
  };

  struct UnknownVersionException : public utils::BasicException {
    UnknownVersionException() : utils::BasicException("Unable to parse the WAL version!") {}
  };

  static Version VersionCheck(std::optional<std::string_view> val) {
    if (val && *val == "V0") return Version::V0;
    throw UnknownVersionException();
  };

  WAL(const std::filesystem::path &dir) : store{dir} {
    if (store.Size() == 0) {  // Fresh store
      store.Put("version", "V0");
      return;
    }
    const auto ver = store.Get("version");
    VersionCheck(ver);
  }

  static auto GenKey(const SystemTransaction &transaction) -> std::string {
    DMG_ASSERT(transaction.delta, "No delta in the system transaction");
    return fmt::format("delta:{}:{}", transaction.system_timestamp, transaction.delta->action);
  }

  /*
   * Delta specializations...
   */
  static auto GenVal(SystemTransaction::Delta::CreateDatabase /*tag*/, const SystemTransaction::Delta &delta) {
    nlohmann::json json;
    json["config"] = delta.config;
    return json.dump();
  }

  std::optional<std::string> Get(const std::string &key = "") const { return store.Get(key); }
  bool Put(const std::string &key, const std::string &val) { return store.Put(key, val); }
  auto begin(const std::string &key = "") const { return store.begin(key); }
  auto end(const std::string &key = "") const { return store.end(key); }

 private:
  kvstore::KVStore store;
};
}  // namespace memgraph::dbms
