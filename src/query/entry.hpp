#pragma once

#include "database/graph_db_accessor.hpp"
#include "query/frontend/opencypher/parser.hpp"

namespace query {

template <typename Stream>
class Engine {
 public:
  Engine() {}
  auto Execute(const std::string &query, GraphDbAccessor &db_accessor,
               Stream &stream) {
    frontend::opencypher::Parser parser(query);
    auto low_level_tree = parser.tree();
    // high level tree
    // typechecked tree
    // logical tree
    // interpret & stream results
  }
};
}
