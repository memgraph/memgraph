#pragma once

#include <string>

#include "i_code_cpu.hpp"
#include "database/db.hpp"

class CodeExecutor
{
public:

    void execute(ICodeCPU *code_cpu)
    {
        code_cpu->name();
        code_cpu->run(db);
    }

private:
    Db db;
};
