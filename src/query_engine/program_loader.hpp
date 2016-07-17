#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#define NOT_LOG_INFO

#include "code_compiler.hpp"
#include "code_generator.hpp"
#include "config/config.hpp"
#include "memgraph_dynamic_lib.hpp"
#include "query_program.hpp"
#include "query_stripper.hpp"
#include "utils/hashing/fnv.hpp"
#include "utils/log/logger.hpp"

using std::string;
using std::cout;
using std::endl;

class ProgramLoader
{
public:
    using sptr_code_lib = std::shared_ptr<CodeLib>;

    ProgramLoader()
        : stripper(make_query_stripper(TK_INT, TK_FLOAT, TK_STR, TK_BOOL))
    {
    }

    auto load(const string &query)
    {
        auto stripped = stripper.strip(query);
        LOG_INFO("stripped_query=" + stripped.query);

        auto hash_string = std::to_string(stripped.hash);
        LOG_INFO("query_hash=" + hash_string);

        auto code_lib_iter = code_libs.find(stripped.hash);

        // code is already compiled and loaded, just return runnable
        // instance
        if (code_lib_iter != code_libs.end()) {
            auto code = code_lib_iter->second->instance();
            return QueryProgram(code, std::move(stripped));
        }

        // code has to be generated, compiled and loaded
        //  TODO load output path from config
        auto base_path = config::Config::instance()[config::COMPILE_CPU_PATH];
        auto path_cpp = base_path + hash_string + ".cpp";
        code_generator.generate_cpp(query, stripped.hash, path_cpp);

        //  TODO compile generated code
        auto path_so = base_path + hash_string + ".so";
        code_compiler.compile(path_cpp, path_so);

        // loads dynamic lib and store it
        auto code_lib = load_code_lib(path_so);
        code_libs.insert({{stripped.hash, code_lib}});

        // return instance of runnable code (ICodeCPU)
        LOG_INFO("new code compiled and placed into cache");
        return QueryProgram(code_lib->instance(), std::move(stripped));
    }

private:
    //  TODO somehow remove int.. from here
    QueryStripper<int, int, int, int> stripper;
    // TODO ifdef MEMGRAPH64 problem, how to use this kind
    // of ifdef functions?
    // uint64_t depends on fnv function
    std::unordered_map<uint64_t, sptr_code_lib> code_libs;

    CodeGenerator code_generator;
    CodeCompiler code_compiler;

    sptr_code_lib load_code_lib(const string &path)
    {
        sptr_code_lib code_lib = std::make_shared<CodeLib>(path);
        code_lib->load();
        return code_lib;
    }
};
