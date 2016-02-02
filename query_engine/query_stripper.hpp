#pragma once

#include <string>
#include <regex>

#include "cypher/tokenizer/cypher_lexer.hpp"
#include "cypher/cypher.h"

class QueryStripper
{
public:

    // TODO: extract parameters

    std::string strip(const std::string& query)
    {
        auto tokenizer = lexer.tokenize(query);
        std::string stripped = "";
        int counter = 0;
        while (auto token = tokenizer.lookup())
        {
            // TODO: do this more generic via template metaprogramming
            if (token.id == TK_STR || token.id == TK_INT ||
                token.id == TK_FLOAT) {
                stripped += "@" + std::to_string(counter++);
            } else {
                stripped += token.value;
            }
        }
        return stripped;
    }

private:
    CypherLexer lexer;
};
