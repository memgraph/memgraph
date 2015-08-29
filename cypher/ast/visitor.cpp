#include <iostream>

#include "ast_visitor.hpp"
#include "ast_node.hpp"
#include "ast_echo.hpp"
#include "boolean.hpp"

int main(void)
{
    Token t;
    AstNode* root = new Boolean(t);

    AstEcho echo;
    root->accept(echo);

    return 0;
}
