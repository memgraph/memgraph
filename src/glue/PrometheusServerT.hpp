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

#include "communication/http/server.hpp"
#include "http_handlers/prometheus_service.hpp"
#include "metrics/prometheus_metrics.hpp"

extern template class memgraph::communication::http::Server<memgraph::http::PrometheusRequestHandler,
                                                            memgraph::metrics::PrometheusMetrics>;

namespace memgraph::glue {

using PrometheusServerT = memgraph::communication::http::Server<memgraph::http::PrometheusRequestHandler,
                                                                memgraph::metrics::PrometheusMetrics>;

}  // namespace memgraph::glue
