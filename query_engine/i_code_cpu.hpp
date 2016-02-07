#pragma once

#include <string>

#include "database/db.hpp"

class ICodeCPU
{
public:
    virtual std::string query() const = 0;
    virtual void run(Db& db) const = 0;
    virtual ~ICodeCPU() {}
};

typedef ICodeCPU* (*produce_t)();
typedef void (*destruct_t)(ICodeCPU*);
