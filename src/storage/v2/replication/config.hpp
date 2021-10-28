// Copyright 2021 Memgraph Ltd.
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
#include <optional>
#include <string>

namespace storage::replication {
struct ReplicationClientConfig {
  std::optional<double> timeout;

  struct SSL {
    std::string key_file = "";
    std::string cert_file = "";
  };

  std::optional<SSL> ssl;
};

struct ReplicationServerConfig {
  struct SSL {
    std::string key_file;
    std::string cert_file;
    std::string ca_file;
    bool verify_peer;
  };

  std::optional<SSL> ssl;
};
}  // namespace storage::replication
