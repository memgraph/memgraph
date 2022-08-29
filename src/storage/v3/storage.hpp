// Copyright 2022 Memgraph Ltd.
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

#include <vector>

#include <boost/asio/thread_pool.hpp>

#include "storage/v3/shard.hpp"

namespace memgraph::storage::v3 {

// class Storage {
//  public:
//   explicit Storage(Config config);
//   // Interface toward shard manipulation
//   // Shard handler -> will use rsm client

//  private:
//   std::vector<Shard> shards_;
//   boost::asio::thread_pool shard_handlers_;
//   Config config_;
// };

}  // namespace memgraph::storage::v3
