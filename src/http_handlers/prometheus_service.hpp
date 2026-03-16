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

#include <prometheus/detail/builder.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>
#include <prometheus/text_serializer.h>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include "dbms/dbms_handler.hpp"

namespace memgraph::http {

class PrometheusRequestHandler final {
 public:
  explicit PrometheusRequestHandler(dbms::DbmsHandler *dbms_handler)
      : dbms_handler_(dbms_handler),
        vertex_count_family_{prometheus::BuildGauge()
                                 .Name("memgraph_vertex_count")
                                 .Help("Number of vertices in the database")
                                 .Register(registry_)},
        edge_count_family_{prometheus::BuildGauge()
                               .Name("memgraph_edge_count")
                               .Help("Number of edges in the database")
                               .Register(registry_)},
        disk_usage_family_{prometheus::BuildGauge()
                               .Name("memgraph_disk_usage_bytes")
                               .Help("Disk usage of the database in bytes")
                               .Register(registry_)},
        memory_res_family_{prometheus::BuildGauge()
                               .Name("memgraph_memory_res_bytes")
                               .Help("Resident memory usage of the database in bytes")
                               .Register(registry_)} {}

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

    dbms_handler_->ForEach([this](dbms::DatabaseAccess db_acc) {
      auto const info = db_acc->storage()->GetBaseInfo();
      auto const db_name = db_acc->name();
      prometheus::Labels const labels{{"database", db_name}};

      vertex_count_family_.Add(labels).Set(static_cast<double>(info.vertex_count));
      edge_count_family_.Add(labels).Set(static_cast<double>(info.edge_count));
      disk_usage_family_.Add(labels).Set(static_cast<double>(info.disk_usage));
      memory_res_family_.Add(labels).Set(static_cast<double>(info.memory_res));
    });

    prometheus::TextSerializer serializer;
    std::ostringstream oss;
    serializer.Serialize(oss, registry_.Collect());

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
  dbms::DbmsHandler *const dbms_handler_;
  prometheus::Registry registry_;
  prometheus::Family<prometheus::Gauge> &vertex_count_family_;
  prometheus::Family<prometheus::Gauge> &edge_count_family_;
  prometheus::Family<prometheus::Gauge> &disk_usage_family_;
  prometheus::Family<prometheus::Gauge> &memory_res_family_;
};

}  // namespace memgraph::http
