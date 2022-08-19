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

#include <map>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "../rsm/shard_rsm.hpp"

namespace memgraph::io {

using memgraph::io::rsm::StorageRsm;

// TODO(gabor) make this work with a threadpool
struct RsmMap {
  std::map<boost::uuids::uuid, StorageRsm> map;
};

}  // namespace memgraph::io
