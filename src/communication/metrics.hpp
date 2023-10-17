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

#include <atomic>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

#include <json/json.hpp>

namespace memgraph::communication {

class BoltMetrics {
 public:
  enum class ConnectionType { kAnonymous = 0, kBasic, Count };
  static constexpr std::array<std::string_view, (int)ConnectionType::Count> ct_to_str = {"anonymous", "basic"};
  static std::string ConnectionTypeStr(ConnectionType type) { return std::string(ct_to_str[(int)type]); }
  class Metrics;

  struct Info {
    explicit Info(std::string name) : name(std::move(name)) {}
    Info(std::string name, std::string bolt_v, std::vector<std::string> supported_bolt_v)
        : name(std::move(name)), bolt_v(std::move(bolt_v)), supported_bolt_v(std::move(supported_bolt_v)) {}

    const std::string name;                                        //!< Driver name
    std::string bolt_v;                                            //!< Bolt version used
    std::vector<std::string> supported_bolt_v;                     //!< Bolt versions supported by the driver
    std::atomic_int connection_types[(int)ConnectionType::Count];  //!< Authentication used when connecting
    std::atomic_int sessions;                                      //!< Number of sessions using the same driver
    std::atomic_int queries;                                       //!< Queries executed by the driver

    nlohmann::json ToJson() const;

   private:
    friend class Metrics;
    std::atomic_int concurrent_users;
  };

  class Metrics {
    friend class BoltMetrics;
    explicit Metrics(Info &info) : info_(&info) { ++info_->concurrent_users; }

   public:
    ~Metrics() {
      if (info_) --info_->concurrent_users;
    }

    Metrics(const Metrics &other) : info_(other.info_) { ++info_->concurrent_users; }
    Metrics(Metrics &&other) noexcept : info_(other.info_) { other.info_ = nullptr; }
    Metrics &operator=(const Metrics &other) {
      if (this != &other) {
        if (info_) --info_->concurrent_users;
        info_ = other.info_;
        if (info_) ++info_->concurrent_users;
      }
      return *this;
    }
    Metrics &operator=(Metrics &&other) noexcept {
      if (this != &other) {
        if (info_) --info_->concurrent_users;
        info_ = other.info_;
        other.info_ = nullptr;  // Invalidate the source object
      }
      return *this;
    }

    Info *operator->() { return info_; }

    Info *info_;
  };

  Metrics Add(std::string name) {
    std::unique_lock<std::mutex> l(mtx);
    auto key = name;
    auto [it, _] = info.emplace(std::piecewise_construct, std::forward_as_tuple(std::move(key)),
                                std::forward_as_tuple(std::move(name)));
    return Metrics(it->second);
  }

  Metrics Add(std::string name, std::string bolt_v, std::vector<std::string> supported_bolt_v) {
    std::unique_lock<std::mutex> l(mtx);
    auto key = name;
    auto [it, _] = info.emplace(std::piecewise_construct, std::forward_as_tuple(std::move(key)),
                                std::forward_as_tuple(std::move(name), std::move(bolt_v), std::move(supported_bolt_v)));
    return Metrics(it->second);
  }

  nlohmann::json ToJson() {
    std::unique_lock<std::mutex> l(mtx);
    auto res = nlohmann::json::array();
    for (const auto &[_, client_info] : info) {
      res.push_back(client_info.ToJson());
    }
    return res;
  }

 private:
  mutable std::mutex mtx;
  std::map<std::string, Info> info;
};

extern BoltMetrics bolt_metrics;

}  // namespace memgraph::communication
