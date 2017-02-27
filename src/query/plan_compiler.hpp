#pragma once

#include <string>

#include "logging/default.hpp"
#include "logging/loggable.hpp"
#include "query/exception/plan_compilation.hpp"
#include "utils/string/join.hpp"

// TODO:
//     * all libraries have to be compiled in the server compile time
//     * compile command has to be generated

/**
 * Compiles code into shared object (.so)
 */
class PlanCompiler : public Loggable {
 public:
  PlanCompiler() : Loggable("PlanCompiler") {}

  /**
   * Compiles in_file into out_file (.cpp -> .so)
   *
   * @param in_file C++ file that can be compiled into dynamic lib
   * @param out_file dynamic lib (on linux .so)
   *
   * @return void
   */
  void compile(const std::string &in_file, const std::string &out_file) {
    std::string flags;

// TODO: sync this with cmake configuration
#ifdef NDEBUG
    flags += " -DNDEBUG -O2";
#endif
#ifdef LOG_NO_TRACE
    flags += " -DLOG_NO_TRACE";
#endif
#ifdef LOG_NO_DEBUG
    flags += " -DLOG_NO_DEBUG";
#endif
#ifdef LOG_NO_INFO
    flags += " -DLOG_NO_INFO";
#endif
#ifdef LOG_NO_WARN
    flags += " -DLOG_NO_WARN";
#endif
#ifdef LOG_NO_ERROR
    flags += " -DLOG_NO_ERROR";
#endif

    // TODO: load from config (generate compile command)
    // generate compile command
    auto compile_command = utils::prints(
        "clang++" + flags,
        "-DHARDCODED_OUTPUT_STREAM",
        // "-std=c++1y -O2 -DNDEBUG",
        "-std=c++1y",    // compile flags
        in_file,         // input file
        "-o", out_file,  // ouput file
        "-I./include",   // include paths
        "-I../include", "-I../../include", "-I../../../include",
        "-I../libs/fmt", "-I../../libs/fmt", "-I../../../libs/fmt",
        "-L./ -L../ -L../../", "-lmemgraph_pic",
        "-shared -fPIC"  // shared library flags
        );

    logger.debug("compile command -> {}", compile_command);

    // synchronous call
    auto compile_status = system(compile_command.c_str());

    logger.debug("compile status {}", compile_status);

    // if compilation has failed throw exception
    if (compile_status != 0) {
      logger.debug("FAIL: Query Code Compilation: {} -> {}", in_file, out_file);
      throw PlanCompilationException(
          "Code compilation error. Generated code is not compilable or "
          "compilation settings are wrong");
    }

    logger.debug("SUCCESS: Query Code Compilation: {} -> {}", in_file,
                 out_file);
  }
};
