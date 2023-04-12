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

namespace memgraph::communication::http {

namespace beast = boost::beast;
namespace http = beast::http;

struct MetricsResponse {
 public:
  nlohmann::json AsJson() {
    auto metrics_response = nlohmann::json();
    metrics_response["size"] = size;
    return metrics_response;
  }

  uint64_t size;
};

class MetricsService {
 public:
  MetricsResponse GetMetrics() { return MetricsResponse{.size = 5}; }
};

class MetricsRequestHandler final {
 public:
  template <typename... Args>
  static std::shared_ptr<MetricsRequestHandler> Create(Args &&...args) {
    return std::shared_ptr<MetricsRequestHandler>(new MetricsRequestHandler(std::forward<Args>(args)...));
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

    auto service_response = service_->GetMetrics().AsJson();
    body.append(service_response.dump());

    // Cache the size since we need it after the move
    auto const size = body.size();

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
  explicit MetricsRequestHandler() : service_(std::make_unique<MetricsService>()) {
    spdlog::info("Basic request handler started!");
  }

  std::unique_ptr<MetricsService> service_;
};
}  // namespace memgraph::communication::http
