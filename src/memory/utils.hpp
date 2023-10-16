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

#include <sstream>
#include <thread>

inline std::string get_string_thread_id(const std::thread::id &thread_id) {
  std::ostringstream oss;
  oss << thread_id;
  return oss.str();
}

inline std::string get_thread_id() { return get_string_thread_id(std::this_thread::get_id()); }

// TODO (af) think if following implementation would make sense
/*
std::string get_thread_id() {
  static thread_local std::thread::id current_thread_id = std::this_thread::get_id();
  // figure out how to cache this part
  std::ostringstream oss;
  oss << current_thread_id;
  return oss.str();
}
*/
