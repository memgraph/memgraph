#pragma once

#include <string>

#include "i_code_cpu.hpp"
#include "database/db.hpp"
#include "utils/log/logger.hpp"

class CodeExecutor
{
public:

    void execute(ICodeCPU *code_cpu)
    {
        log.info(code_cpu->query());
        code_cpu->run(db);
    }

private:
    Db db;
    Logger log;
};
