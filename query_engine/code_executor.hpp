#pragma once

#include <string>

#include "i_code_cpu.hpp"
#include "database/db.hpp"
#include "utils/log/logger.hpp"

//  preparations before execution
//  execution
//  postprocess the results

using std::cout;
using std::endl;

class CodeExecutor
{
public:
    void execute(ICodeCPU *code_cpu)
    {
        auto result = code_cpu->run(db);
        cout << result->result << endl;
    }

private:
    Db db;
};
