// Copyright 2026 Memgraph Ltd.
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
#include <string>
#include <variant>

#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <nlohmann/json.hpp>

#include "metrics/prometheus_metrics.hpp"

namespace memgraph::http {

// Serves the legacy JSON metrics endpoint. Deprecated in favour of OpenMetrics.
class MetricsRequestHandler final {
 public:
  explicit MetricsRequestHandler(metrics::PrometheusMetrics *metrics) : metrics_(metrics) {}

  MetricsRequestHandler(MetricsRequestHandler const &) = delete;
  MetricsRequestHandler &operator=(MetricsRequestHandler const &) = delete;
  MetricsRequestHandler(MetricsRequestHandler &&) = delete;
  MetricsRequestHandler &operator=(MetricsRequestHandler &&) = delete;
  ~MetricsRequestHandler() = default;

  template <class Body, class Allocator>
  void HandleRequest(boost::beast::http::request<Body, boost::beast::http::basic_fields<Allocator>> &&req,
                     std::function<void(boost::beast::http::response<boost::beast::http::string_body>)> &&send) {
    auto const bad_request = [&req](std::string_view why) {
      nlohmann::json error;
      error["error"] = std::string(why);
      boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::bad_request,
                                                                        req.version()};
      res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
      res.set(boost::beast::http::field::content_type, "application/json");
      res.keep_alive(req.keep_alive());
      res.body() = error.dump();
      res.prepare_payload();
      return res;
    };

    if (req.method() != boost::beast::http::verb::get) {
      return send(bad_request("Unknown HTTP-method"));
    }

    if (req.target().empty() || req.target()[0] != '/' || req.target().find("..") != boost::beast::string_view::npos) {
      return send(bad_request("Illegal request-target"));
    }

    metrics_->UpdateGauges();

    auto const metric_infos = metrics_->GetGlobalMetricsInfoForLegacyJson();

    // Translation table for backwards compatibility with the legacy JSON format.
    // A source metric may appear multiple times to emit into multiple JSON groups.
    // Remove when JSON support is dropped.
    struct Remap {
      std::string_view name;
      std::string_view type;
      std::string_view json_name;
      std::string_view json_type;
    };

    static constexpr std::array kRemaps = std::to_array<Remap>({
        {"VertexCount", "General", "vertex_count", "General"},
        {"EdgeCount", "General", "edge_count", "General"},
        {"AverageDegree", "General", "average_degree", "General"},
        {"DiskUsage", "Memory", "disk_usage", "General"},
        {"MemoryRes", "Memory", "memory_usage", "General"},
        {"PeakMemoryRes", "Memory", "peak_memory_usage", "General"},
        {"PeakMemoryRes", "Memory", "PeakMemoryRes", "Memory"},
        {"UnreleasedDeltaObjects", "Memory", "unreleased_delta_objects", "General"},
        {"UnreleasedDeltaObjects", "Memory", "UnreleasedDeltaObjects", "Memory"},
        {"SocketConnect_us_50p", "HighAvailability", "SocketConnect_us_50p", "General"},
        {"SocketConnect_us_90p", "HighAvailability", "SocketConnect_us_90p", "General"},
        {"SocketConnect_us_99p", "HighAvailability", "SocketConnect_us_99p", "General"},
    });

    auto const emit = [&](nlohmann::json &out,
                          std::string_view json_type,
                          std::string_view json_name,
                          metrics::MetricInfo const &info) {
      std::visit([&](auto v) { out[json_type][json_name] = v; }, info.value);
    };

    nlohmann::json result;
    for (auto const &info : metric_infos) {
      bool remapped = false;
      for (auto const &r : kRemaps) {
        if (r.name == info.name && r.type == info.type) {
          emit(result, r.json_type, r.json_name, info);
          remapped = true;
        }
      }
      if (!remapped) emit(result, info.type, info.name, info);
    }

    auto body = result.dump();
    auto const size = body.size();

    boost::beast::http::response<boost::beast::http::string_body> res{
        std::piecewise_construct,
        std::make_tuple(std::move(body)),
        std::make_tuple(boost::beast::http::status::ok, req.version())};
    res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(boost::beast::http::field::content_type, "application/json");
    res.content_length(size);
    res.keep_alive(req.keep_alive());
    return send(std::move(res));
  }

 private:
  metrics::PrometheusMetrics *metrics_;
};

}  // namespace memgraph::http
