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

#include <spdlog/spdlog.h>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <json/json.hpp>

#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::http {

namespace beast = boost::beast;
namespace http = beast::http;

struct MetricsResponse {
 public:
  nlohmann::json AsJson() {
    auto metrics_response = nlohmann::json();
    metrics_response["vertex_count"] = vertex_count;
    metrics_response["edge_count"] = edge_count;
    metrics_response["average_degree"] = average_degree;
    metrics_response["memory_usage"] = memory_usage;
    metrics_response["disk_usage"] = disk_usage;
    return metrics_response;
  }

  uint64_t vertex_count;
  uint64_t edge_count;
  double average_degree;
  uint64_t memory_usage;
  uint64_t disk_usage;
};

template <typename TSessionData>
class MetricsService {
 public:
  explicit MetricsService(TSessionData *data)
      : db_(data->db), interpreter_context_(data->interpreter_context), interpreter_(data->interpreter_context) {}
  MetricsResponse GetMetrics() {
    auto info = db_->GetInfo();
    return MetricsResponse{.vertex_count = info.vertex_count,
                           .edge_count = info.edge_count,
                           .average_degree = info.average_degree,
                           .memory_usage = info.memory_usage,
                           .disk_usage = info.disk_usage};
  }

 private:
  const storage::Storage *db_;
  query::InterpreterContext *interpreter_context_;
  query::Interpreter interpreter_;
};

template <typename TSessionData>
class MetricsRequestHandler final {
 public:
  explicit MetricsRequestHandler(TSessionData *data) : service_(data) {
    spdlog::info("Basic request handler started!");
  }

  MetricsRequestHandler(const MetricsRequestHandler &) = delete;
  MetricsRequestHandler(MetricsRequestHandler &&) = delete;
  MetricsRequestHandler &operator=(const MetricsRequestHandler &) = delete;
  MetricsRequestHandler &operator=(MetricsRequestHandler &&) = delete;
  ~MetricsRequestHandler() = default;

  template <class Body, class Allocator>
  void HandleRequest(http::request<Body, http::basic_fields<Allocator>> &&req,
                     std::function<void(http::response<http::string_body>)> &&send) {
    auto response_json = nlohmann::json();
    // Returns a bad request response
    auto const bad_request = [&req, &response_json](beast::string_view why) {
      response_json["error"] = std::string(why);

      http::response<http::string_body> res{http::status::bad_request, req.version()};
      res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
      res.set(http::field::content_type, "application/json");
      res.keep_alive(req.keep_alive());
      res.body() = response_json.dump();
      res.prepare_payload();
      return res;
    };

    // Make sure we can handle the method
    if (req.method() != http::verb::get) {
      return send(bad_request("Unknown HTTP-method"));
    }

    // Request path must be absolute and not contain "..".
    if (req.target().empty() || req.target()[0] != '/' || req.target().find("..") != beast::string_view::npos) {
      return send(bad_request("Illegal request-target"));
    }

    http::string_body::value_type body;

    auto service_response = service_.GetMetrics().AsJson();
    body.append(service_response.dump());

    // Cache the size since we need it after the move
    const auto size = body.size();

    // Respond to GET request
    http::response<http::string_body> res{std::piecewise_construct, std::make_tuple(std::move(body)),
                                          std::make_tuple(http::status::ok, req.version())};
    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(http::field::content_type, "application/json");
    res.content_length(size);
    res.keep_alive(req.keep_alive());
    return send(std::move(res));
  }

 private:
  MetricsService<TSessionData> service_;
};
}  // namespace memgraph::http
