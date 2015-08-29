#ifndef MEMGRAPH_CYPHER_LEXER_CYPHER_LEXER_HPP
#define MEMGRAPH_CYPHER_LEXER_CYPHER_LEXER_HPP

#include "cypher.h"

#include "lexer.hpp"

class CypherLexer : public Lexer
{
public:

    CypherLexer()
    {
        // whitespace
        rule("\\s+", sm.skip());
        
        // special characters
        rule("\\.", TK_DOT);
        rule(",", TK_COMMA);
        rule(":", TK_COLON);
        rule("\\|", TK_PIPE);
        rule("\\{", TK_LCP);
        rule("\\}", TK_RCP);
        rule("\\(", TK_LP);
        rule("\\)", TK_RP);
        rule("\\[", TK_LSP);
        rule("\\]", TK_RSP);

        // operators
        rule("\\+", TK_PLUS);
        rule("-", TK_MINUS);
        rule("\\*", TK_STAR);
        rule("\\/", TK_SLASH);
        rule("%", TK_REM);

        rule(">", TK_GT);
        rule("<", TK_LT);
        rule(">=", TK_GE);
        rule("<=", TK_LE);
        rule("=", TK_EQ);
        rule("<>", TK_NE);

        // constants
        rule("(?i:TRUE)", TK_BOOL);
        rule("(?i:FALSE)", TK_BOOL); 

        // keywords
        rule("(?i:MATCH)", TK_MATCH);
        rule("(?i:WHERE)", TK_WHERE);
        rule("(?i:RETURN)", TK_RETURN);

        rule("(?i:AND)", TK_AND);
        rule("(?i:OR)", TK_OR);

        // string literal TODO single quote escape
        rule("'(.*?)'", TK_STR);
        
        // string literal TODO double quote escape
        rule("\\\"(.*?)\\\"", TK_STR);
        
        // number
        rule("\\d+", TK_INT);
        rule("\\d*[.]?\\d+", TK_FLOAT);

        // identifier
        rule("[_a-zA-Z][_a-zA-Z0-9]{0,30}", TK_IDN);

        build();
    }
};

#endif
