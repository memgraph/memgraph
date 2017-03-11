#pragma once

#include "antlr4-runtime.h"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include <experimental/filesystem>

namespace fs = std::experimental::filesystem;

namespace backend {

namespace cpp {

using namespace antlr4;

class GeneratorException : public BasicException {
public:
  using BasicException::BasicException;
  GeneratorException() : BasicException("") {}
};

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
    throw GeneratorException("unsupported query");
    CypherMainVisitor visitor;
    visitor.visit(tree);
  }
};
}
}
