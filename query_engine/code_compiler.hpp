#pragma once

#include <string>

#include "utils/string/join.hpp"

class CodeCompiler
{
public:
    void compile(const std::string& in_file, const std::string& out_file)
    {
        auto compile_command = utils::prints(
            "clang++",
            "-std=c++1y",
            in_file,
            "-o", out_file,
            "-I../",
            "-shared -fPIC"
        );

        // synchronous call
        system(compile_command.c_str());
    }
};
