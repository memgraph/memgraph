#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <experimental/filesystem>

#include "config/config.hpp"
#include "logging/default.hpp"
#include "query/dynamic_lib.hpp"
#include "query/frontend/cypher.hpp"
#include "query/plan/compiler.hpp"
#include "query/plan/generator.hpp"
#include "query/plan/program.hpp"
#include "query/preprocesor.hpp"
#include "utils/file.hpp"
#include "utils/hashing/fnv.hpp"

namespace fs = std::experimental::filesystem;

template <typename Stream>
class ProgramLoader
{
public:
    using code_lib_t = CodeLib<Stream>;
    using sptr_code_lib = std::shared_ptr<code_lib_t>;
    using query_program_t = QueryProgram<Stream>;

    ProgramLoader() : logger(logging::log->logger("PlanLoader")) {}

    // TODO: decouple load(query) method

    auto load(const uint64_t hash, const fs::path &path)
    {
        // TODO: get lib path (that same folder as path folder or from config)
        // TODO: compile
        // TODO: dispose the old lib
        // TODO: store the compiled lib
    }

    auto load(const std::string &query)
    {
        auto preprocessed = preprocessor.preprocess(query);
        logger.debug("stripped_query = {}", preprocessed.query);
        logger.debug("query_hash = {}", std::to_string(preprocessed.hash));

        auto code_lib_iter = code_libs.find(preprocessed.hash);

        // code is already compiled and loaded, just return runnable
        // instance
        if (code_lib_iter != code_libs.end()) {
            auto code = code_lib_iter->second->instance();
            return query_program_t(code, std::move(preprocessed));
        }

        auto base_path = CONFIG(config::COMPILE_CPU_PATH);
        auto hash_string = std::to_string(preprocessed.hash);
        auto path_cpp = base_path + hash_string + ".cpp";
        auto hard_code_cpp = base_path + "hardcode/" + hash_string + ".cpp";
        auto stripped_space = preprocessor.strip_space(query);

        // cpp files in the hardcode folder have bigger priority then
        // other cpp files
        if (!utils::fexists(hard_code_cpp)) {
            plan_generator.generate_plan(stripped_space.query,
                                         preprocessed.hash, path_cpp);
        }

        // compile the code
        auto path_so = base_path + hash_string + ".so";

        // hardcoded queries are compiled to the same folder as generated
        // queries (all .so files are in the same folder)
        if (utils::fexists(hard_code_cpp)) {
            plan_compiler.compile(hard_code_cpp, path_so);
        } else {
            plan_compiler.compile(path_cpp, path_so);
        }

        // loads dynamic lib and store it
        auto code_lib = load_code_lib(path_so);
        code_libs.insert({{preprocessed.hash, code_lib}});

        // return an instance of runnable code (ICodeCPU)
        return query_program_t(code_lib->instance(), std::move(preprocessed));
    }

protected:
    Logger logger;

private:
    // TODO ifdef MEMGRAPH64 problem, how to use this kind
    // of ifdef functions?
    // uint64_t depends on fnv function
    // TODO: faster datastructure
    std::unordered_map<uint64_t, sptr_code_lib> code_libs;

    QueryPreprocessor preprocessor;

    // TODO: compile time switch between frontends and backends
    PlanGenerator<cypher::Frontend, CypherBackend<Stream>> plan_generator;

    PlanCompiler plan_compiler;

    sptr_code_lib load_code_lib(const std::string &path)
    {
        sptr_code_lib code_lib = std::make_shared<CodeLib<Stream>>(path);
        code_lib->load();
        return code_lib;
    }
};
