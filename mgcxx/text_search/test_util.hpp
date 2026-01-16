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

#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <sstream>
#include <vector>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include "text_search.hpp"

nlohmann::json dummy_mappings1() {
  nlohmann::json mappings = {};
  mappings["properties"] = {};
  mappings["properties"]["metadata"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
  mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
  return mappings;
}
std::vector<mgcxx::text_search::DocumentInput> dummy_data1(uint64_t docs_no = 1, uint64_t props_no = 1) {
  std::vector<mgcxx::text_search::DocumentInput> docs;
  for (uint64_t doc_index = 0; doc_index < docs_no; ++doc_index) {
    nlohmann::json data = {};
    nlohmann::json props = {};
    for (uint64_t prop_index = 0; prop_index < props_no; ++prop_index) {
      props[fmt::format("key{}", prop_index)] = fmt::format("value{} is AWESOME", prop_index);
    }
    data["data"] = props;
    data["metadata"] = {};
    data["metadata"]["gid"] = doc_index;
    data["metadata"]["txid"] = doc_index;
    data["metadata"]["deleted"] = false;
    data["metadata"]["is_node"] = false;
    mgcxx::text_search::DocumentInput doc = {
        .data = data.dump(),
    };
    docs.push_back(doc);
  }
  return docs;
}

nlohmann::json dummy_mappings2() {
  nlohmann::json mappings = {};
  mappings["properties"] = {};
  mappings["properties"]["gid"] = {{"type", "u64"}, {"fast", true}, {"stored", true}, {"indexed", true}};
  mappings["properties"]["data"] = {{"type", "json"}, {"fast", true}, {"stored", true}, {"text", true}};
  return mappings;
}
std::vector<mgcxx::text_search::DocumentInput> dummy_data2(uint64_t docs_no = 1, uint64_t props_no = 1) {
  std::vector<mgcxx::text_search::DocumentInput> docs;
  for (uint64_t doc_index = 0; doc_index < docs_no; ++doc_index) {
    nlohmann::json data = {};
    data["gid"] = doc_index;
    nlohmann::json props = {};
    for (uint64_t prop_index = 0; prop_index < props_no; ++prop_index) {
      props[fmt::format("key{}", prop_index)] = fmt::format("value{} is AWESOME", prop_index);
    }
    data["data"] = props;
    mgcxx::text_search::DocumentInput doc = {
        .data = data.dump(),
    };
    docs.push_back(doc);
  }
  return docs;
}

std::ostream &operator<<(std::ostream &os, const mgcxx::text_search::DocumentOutput &element) {
  os << element.data;
  return os;
}

auto now() { return std::chrono::steady_clock::now(); }
template <typename T>
auto print_time_diff(std::string_view prefix, T start, T end) {
  std::cout << prefix << " dt = " << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()
            << "[µs]" << std::endl;
  // << "[ms]" << std::endl;
}
template <typename T>
auto measure_time_diff(std::string_view prefix, std::function<T()> f) {
  auto start = now();
  T result = f();
  auto end = now();
  print_time_diff(prefix, start, end);
  return result;
}

std::filesystem::path create_temporary_directory(std::string_view file_prefix = "", std::string_view file_suffix = "",
                                                 unsigned long long max_tries = 100) {
  auto tmp_dir = std::filesystem::temp_directory_path();
  unsigned long long i = 0;
  std::random_device dev;
  std::mt19937 prng(dev());
  std::uniform_int_distribution<uint64_t> rand(0);
  std::filesystem::path path;
  while (true) {
    std::stringstream ss;
    ss << file_prefix << rand(prng) << file_suffix;
    path = tmp_dir / ss.str();
    if (std::filesystem::create_directory(path)) {
      break;
    }
    if (i == max_tries) {
      throw std::runtime_error("could not find non-existing directory");
    }
    i++;
  }
  return path;
}
