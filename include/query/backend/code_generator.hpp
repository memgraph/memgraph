#pragma once

#include <stdexcept>
#include <string>

#include "query/backend/backend.hpp"

class CodeGenerator : public Backend
{
private:
    struct Code
    {
    public:
        void println(const std::string &line)
        {
            code.append(line + "\n");
            line_no++;
        }

        void save(const std::string &)
        {
            throw std::runtime_error("TODO: implementation");
        }

        void reset() { code = ""; }

        std::string code;
        uint64_t line_no;
    };

protected:
    Code code;

public:
    void emit(const std::string &line) { code.println(line); }

    void save(const std::string &path) { code.save(path); }

    void reset() { code.reset(); }
};
