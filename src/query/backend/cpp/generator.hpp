#pragma once

#include <experimental/filesystem>
#include "antlr4-runtime.h"
#include "query/backend/cpp/cypher_main_visitor.hpp"

namespace fs = std::experimental::filesystem;

namespace backend {

namespace cpp {

using namespace antlr4;

/**
 * Traverse Antlr tree::ParseTree generated from Cypher grammar and generate
 * C++.
 */
class Generator {
 public:
  /**
   * Generates cpp code inside file on the path.
   */
  Generator(tree::ParseTree *tree, const std::string &query,
            const uint64_t stripped_hash, const fs::path &path) {
    CypherMainVisitor visitor;
    visitor.visit(tree);
    throw std::runtime_error("TODO: implementation");
  }
};
}
}
