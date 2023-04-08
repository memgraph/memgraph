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

#include <iostream>

#include "utils/shared_library_handle.hpp"

// APPLE CLANG 14: clang++ -I../../src -std=c++20 -o a.out shared_library_handle.cpp

using namespace memgraph::utils;

int main(int argc, char *argv[]) {

#if defined(__APPLE__)
  std::cout << "Apple" << std::endl;
  LinuxDLHandle handle("test", 0);
#elif defined(__linux__)
  std::cout << "Linux" << std::endl;
  LinuxDLHandle handle("test", 0);
#elif defined(_WIN32)
  std::cout << "Windows" << std::endl;
  WindowsDLLHandle handle("test", 0);
#else
  std::cout << "Unsupportd platform" << std::endl;
#endif

  return 0;
}
