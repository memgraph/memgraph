#pragma once

#include "config/config.hpp"
#include "query/language/cypher/ast/ast.hpp"
#include "query/language/cypher/compiler.hpp"
#include "logging/loggable.hpp"
#include "template_engine/engine.hpp"
#include "utils/string/file.hpp"
#include "utils/type_discovery.hpp"

template <typename Frontend, typename Backend>
class PlanGenerator : public Loggable
{
public:
    PlanGenerator() : Loggable("PlanGenerator") {}

    void generate_plan(const std::string &query, const uint64_t stripped_hash,
                       const std::string &path)
    {
        // TODO: multithread environment TEST
        // multiple connections for the same query at the beginning
        auto ir = frontend.generate_ir(query);
        backend.generate_code(ir, query, stripped_hash, path);
    }

private:
    Frontend frontend;
    Backend backend;
};
