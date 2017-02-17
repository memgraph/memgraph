#pragma once

#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;

namespace backend {

namespace cpp {

/**
 * Traverse AST and generate C++
 */
class Generator {
 public:
  /**
   * Generates cpp code inside file on the path.
   * 
   * @tparam Ast type of AST structure
   */
  template <typename Ast>
  void generate_plan(const Ast &ast, const std::string &query, 
                     const uint64_t stripped_hash, const fs::path &path) {
    throw std::runtime_error("TODO: implementation");
  }
};
}
}
