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

#include "communication/v2/server.hpp"
#include "glue/SessionHL.hpp"

#ifdef MG_ENTERPRISE
#include "dbms/session_context_handler.hpp"
#else
#include "dbms/session_context.hpp"
#endif

namespace memgraph {
#ifdef MG_ENTERPRISE
extern template class memgraph::communication::v2::Server<memgraph::glue::SessionHL,
                                                          memgraph::dbms::SessionContextHandler>;
using ServerT = memgraph::communication::v2::Server<memgraph::glue::SessionHL, memgraph::dbms::SessionContextHandler>;
#else
extern template class memgraph::communication::v2::Server<memgraph::glue::SessionHL, memgraph::dbms::SessionContext>;
using ServerT = memgraph::communication::v2::Server<memgraph::glue::SessionHL, memgraph::dbms::SessionContext>;
#endif
}  // namespace memgraph
