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
  std::cout << "Performed " << REPEATS << " strip ops, each took "
            << elapsed_secs / REPEATS * 1000 << "ms" << std::endl;
}
