#pragma once

#include <string>
#include <unordered_map>
#include <memory>

#include "memgraph_dynamic_lib.hpp"
#include "query_stripper.hpp"
#include "code_compiler.hpp"
#include "code_generator.hpp"
#include "utils/hashing/fnv.hpp"

using std::string;
using std::cout;
using std::endl;

class CodeLoader
{    
public:

    using sptr_code_lib = std::shared_ptr<CodeLib>;

    CodeLoader()
        : stripper(make_query_stripper(TK_INT, TK_FLOAT, TK_STR))
    {
    }

    ICodeCPU* load_code_cpu(const string& query)
    {
        //  TODO implement me
        //  for now returns already compiled code

        auto stripped = stripper.strip(query);
        //  TODO move to logger
        cout << "Stripped query is: " << stripped << endl;
        auto stripped_hash = fnv(stripped);
        //  TODO move to logger
        cout << "Query hash is: " << stripped_hash << endl;
        
        auto code_lib = load_code_lib("./compiled/cpu/create_return.so");
        code_libs.insert({{stripped_hash, code_lib}});

        return code_lib->instance();
    }

private:
    //  TODO somehow remove int.. from here
    QueryStripper<int, int, int> stripper;
    // TODO ifdef MEMGRAPH64 problem, how to use this kind
    // of ifdef functions?
    // uint64_t depends on fnv function
    std::unordered_map<uint64_t, sptr_code_lib> code_libs;

    CodeGenerator code_generator;
    CodeCompiler code_compiler;

    sptr_code_lib load_code_lib(const string& path)
    {
        sptr_code_lib code_lib = std::make_shared<CodeLib>(path);
        code_lib->load();
        return code_lib;
    }
};
