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

//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 07.03.17.
//

#include <ctime>
#include <iostream>

#include "query/frontend/stripped.hpp"

int main(int argc, const char **a) {
  if (argc < 2) {
    std::cout << "Provide a query string as input" << std::endl;
    return 1;
  }

  const char *query = a[1];
  const int REPEATS = 100;

  clock_t begin = clock();
  for (int i = 0; i < REPEATS; ++i) {
    query::frontend::StrippedQuery(std::string(query));
  }
  clock_t end = clock();

  double elapsed_secs = double(end - begin) / CLOCKS_PER_SEC;
  std::cout << "Performed " << REPEATS << " strip ops, each took " << elapsed_secs / REPEATS * 1000 << "ms"
            << std::endl;
}
