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
#include <sstream>

#include <prometheus/registry.h>
#include <prometheus/text_serializer.h>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include "metrics/prometheus_metrics.hpp"

namespace memgraph::http {

class PrometheusRequestHandler final {
 public:
  explicit PrometheusRequestHandler(metrics::PrometheusMetrics *prometheus_metrics)
      : prometheus_metrics_(prometheus_metrics) {}

  PrometheusRequestHandler(PrometheusRequestHandler const &) = delete;
  PrometheusRequestHandler(PrometheusRequestHandler &&) = delete;
  PrometheusRequestHandler &operator=(PrometheusRequestHandler const &) = delete;
  PrometheusRequestHandler &operator=(PrometheusRequestHandler &&) = delete;
  ~PrometheusRequestHandler() = default;

  template <class Body, class Allocator>
  void HandleRequest(boost::beast::http::request<Body, boost::beast::http::basic_fields<Allocator>> &&req,
                     std::function<void(boost::beast::http::response<boost::beast::http::string_body>)> &&send) {
    auto const bad_request = [&req](std::string_view why) {
      boost::beast::http::response<boost::beast::http::string_body> res{boost::beast::http::status::bad_request,
                                                                        req.version()};
      res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
      res.set(boost::beast::http::field::content_type, "text/plain");
      res.keep_alive(req.keep_alive());
      res.body() = std::string(why);
      res.prepare_payload();
      return res;
    };

    if (req.method() != boost::beast::http::verb::get) {
      return send(bad_request("Unknown HTTP-method"));
    }

    if (req.target().empty() || req.target()[0] != '/' || req.target().find("..") != boost::beast::string_view::npos) {
      return send(bad_request("Illegal request-target"));
    }

    prometheus_metrics_->UpdateGauges();

    prometheus::TextSerializer serializer;
    std::ostringstream oss;
    serializer.Serialize(oss, prometheus_metrics_->registry().Collect());

    auto body = oss.str();
    auto const size = body.size();

    boost::beast::http::response<boost::beast::http::string_body> res{
        std::piecewise_construct,
        std::make_tuple(std::move(body)),
        std::make_tuple(boost::beast::http::status::ok, req.version())};
    res.set(boost::beast::http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(boost::beast::http::field::content_type, "text/plain; version=0.0.4");
    res.content_length(size);
    res.keep_alive(req.keep_alive());
    return send(std::move(res));
  }

 private:
  metrics::PrometheusMetrics *const prometheus_metrics_;
};

}  // namespace memgraph::http
