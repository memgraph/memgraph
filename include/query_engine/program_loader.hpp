#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "config/config.hpp"
#include "query_engine/code_compiler.hpp"
#include "query_engine/code_generator.hpp"
#include "query_engine/memgraph_dynamic_lib.hpp"
#include "query_engine/query_program.hpp"
#include "query_engine/query_stripper.hpp"
#include "utils/hashing/fnv.hpp"
#include "logging/default.hpp"

using std::string;

template <typename Stream>
class ProgramLoader
{
public:
    using code_lib_t = CodeLib<Stream>;
    using sptr_code_lib = std::shared_ptr<code_lib_t>;
    using query_program_t = QueryProgram<Stream>;

    ProgramLoader() :
        logger(logging::log->logger("ProgramLoader")),
        stripper(make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL))
    {
    }

    auto load(const string &query)
    {
        auto stripped = stripper.strip(query);
        logger.debug("stripped_query = {}", stripped.query);

        auto hash_string = std::to_string(stripped.hash);
        logger.debug("query_hash = {}", hash_string);

        auto code_lib_iter = code_libs.find(stripped.hash);

        // code is already compiled and loaded, just return runnable
        // instance
        if (code_lib_iter != code_libs.end()) {
            auto code = code_lib_iter->second->instance();
            return query_program_t(code, std::move(stripped));
        }

        // code has to be generated, compiled and loaded
        //  TODO load output path from config
        auto base_path = config::Config::instance()[config::COMPILE_CPU_PATH];
        auto path_cpp = base_path + hash_string + ".cpp";
        auto stripped_space = stripper.strip_space(query);
        code_generator.generate_cpp(stripped_space.query, stripped.hash,
                                    path_cpp);

        auto path_so = base_path + hash_string + ".so";
        code_compiler.compile(path_cpp, path_so);

        // loads dynamic lib and store it
        auto code_lib = load_code_lib(path_so);
        code_libs.insert({{stripped.hash, code_lib}});

        // return an instance of runnable code (ICodeCPU)
        return query_program_t(code_lib->instance(), std::move(stripped));
    }

protected:
    Logger logger;

private:
    //  TODO somehow remove int.. from here
    QueryStripper<int, int, int, int> stripper;

    // TODO ifdef MEMGRAPH64 problem, how to use this kind
    // of ifdef functions?
    // uint64_t depends on fnv function
    std::unordered_map<uint64_t, sptr_code_lib> code_libs;

    CodeGenerator<Stream> code_generator;
    CodeCompiler code_compiler;

    sptr_code_lib load_code_lib(const string &path)
    {
        sptr_code_lib code_lib = std::make_shared<CodeLib<Stream>>(path);
        code_lib->load();
        return code_lib;
    }
};
