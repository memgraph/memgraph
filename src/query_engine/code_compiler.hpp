#pragma once

#include <string>

#include "exceptions/exceptions.hpp"
#include "utils/string/join.hpp"

// TODO:
//     * all libraries have to be compiled in the server compile time
//     * compile command has to be generated
#include <iostream>

class CodeCompiler
{
public:
    void compile(const std::string &in_file, const std::string &out_file)
    {
        // generate compile command
        auto compile_command =
            utils::prints("clang++",
                          // "-std=c++1y -O2 -DNDEBUG",     // compile flags
                          "-std=c++1y -DDEBUG", // compile flags
                          in_file,              // input file
                          "-o", out_file,       // ouput file
                          "-I./include", // include paths (TODO: parameter)
                          "-I./src", "-I../../libs/fmt",
                          "-L./ -lmemgraph_pic",
                          "-shared -fPIC" // shared library flags
                          );

        // synchronous call
        auto compile_status = system(compile_command.c_str());

        // if compilation has failed throw exception
        if (compile_status == -1) {
            throw QueryEngineException("Code compilation error. Generated code "
                                       "is not compilable or compilation "
                                       "settings are wrong");
        }

        // TODO: use logger
        std::cout << fmt::format("SUCCESS: Query Code Compilation: {} -> {}",
                                 in_file, out_file)
                  << std::endl;
    }
};
