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

#include <gflags/gflags.h>
#include <filesystem>
#include <iostream>

#include "kvstore/kvstore.hpp"

DEFINE_string(data_directory, "", "Memgraph data directory");
DEFINE_string(key, "", "Setting key");
DEFINE_string(value, "", "Value to store under the key");
DEFINE_bool(put, false, "Store the value instead of printing the stored one");

/**
 * Reads or writes the settings store directly, the way an older Memgraph version persisted every setting.
 * Prints the stored value and exits with 0; exits with 2 when the key is absent.
 * Memgraph must not be running on the same data directory.
 */
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::kvstore::KVStore store(std::filesystem::path{FLAGS_data_directory} / "settings");
  if (FLAGS_put) {
    return store.Put(FLAGS_key, FLAGS_value) ? 0 : 1;
  }
  const auto value = store.Get(FLAGS_key);
  if (!value) return 2;
  std::cout << *value << '\n';
  return 0;
}
