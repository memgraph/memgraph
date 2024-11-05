// Copyright 2024 Memgraph Ltd.
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

#include <iostream>
#include "kvstore/kvstore.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

DEFINE_string(path, "", "Path to the storage directory.");

int main(int argc, char **argv) {
  gflags::SetVersionString("kvstore_console");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  MG_ASSERT(FLAGS_path != "", "Please specify a path to the KVStore!");

  memgraph::kvstore::KVStore kvstore(std::filesystem::path{FLAGS_path});

  while (true) {
    std::string s;
    std::getline(std::cin, s);
    if (s == "") {
      break;
    }

    auto split = memgraph::utils::Split(memgraph::utils::Trim(s));
    if (split[0] == "list") {
      if (split.size() != 1) {
        std::cout << "`list' takes no arguments!" << std::endl;
      }
      for (auto it = kvstore.begin(); it != kvstore.end(); ++it) {
        std::cout << it->first << " --> " << it->second << std::endl;
      }
    } else if (split[0] == "get") {
      if (split.size() != 2) {
        std::cout << "`get' takes exactly one argument!" << std::endl;
      }
      auto item = kvstore.Get(split[1]);
      if (item != std::nullopt) {
        std::cout << split[1] << " --> " << *item << std::endl;
      } else {
        std::cout << "Key doesn't exist in the database!" << std::endl;
      }
    } else if (split[0] == "put") {
      if (split.size() != 3) {
        std::cout << "`put' takes exactly two arguments!" << std::endl;
      }
      if (kvstore.Put(split[1], split[2])) {
        std::cout << "Success." << std::endl;
      } else {
        std::cout << "Failure!" << std::endl;
      }
    } else if (split[0] == "delete") {
      if (split.size() != 2) {
        std::cout << "`delete' takes exactly one argument!" << std::endl;
      }
      if (kvstore.Delete(split[1])) {
        std::cout << "Success." << std::endl;
      } else {
        std::cout << "Failure!" << std::endl;
      }
    }
    std::cout << std::endl;
  }

  return 0;
}
