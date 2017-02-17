#pragma once

#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;

/**
 * @class PlanGenerator
 *
 * @tparam Frontend defines compiler frontend for query parsing
 *         object of this class must have method with name generate_ast
 * @tparam Backend defines compiler backend for plan gen
 *         object of this class must have method with name generate_code
 *
 */
template <typename Frontend, typename Backend>
class PlanGenerator {
 public:
  /**
   * Generates query plan based on the input query
   */
  void generate_plan(const std::string &query, const uint64_t stripped_hash,
                     const fs::path &path) {
    auto ast = frontend.generate_ast(query);
    backend.generate_plan(ast, query, stripped_hash, path);
  }

 private:
  Frontend frontend;
  Backend backend;
};
