#pragma once

#include <string>

#include "utils/string/join.hpp"

class CodeCompiler
{
public:
    void compile(const std::string &in_file, const std::string &out_file)
    {
        auto compile_command =
            utils::prints("clang++",
                          // "-std=c++1y -O2 -DNDEBUG",     // compile flags
                          "-std=c++1y",   // compile flags
                          in_file,        // input file
                          "-o", out_file, // ouput file
                          "-I../",        // include paths
                          "-shared -fPIC" // shared library flags
                          );

        // synchronous call
        system(compile_command.c_str());
    }
};
