#pragma once

#include <string>

#include "logging/loggable.hpp"
#include "query/language/cypher/compiler.hpp"

// DEPRICATED

namespace cypher
{

class Frontend
{
public:
    Frontend() {}

    auto generate_ir(const std::string& query)
    {
        try {
            return compiler.syntax_tree(query);
        } catch (const BasicException &e) {
            // logger.error("Parsing error while processing query: {}", query);
            // logger.error(std::string(e.what()));
            throw e;
        }
    }

private:
    cypher::Compiler compiler;
};

}
