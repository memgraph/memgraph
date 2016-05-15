#include <iostream>
#include <cassert>

#include "cypher/compiler.hpp"
#include "cypher/debug/tree_print.hpp"

using std::cout;
using std::endl;

int calc(int i)
{
    return i + 1;
}

int main()
{
    // TODO
    // auto print_visitor = new PrintVisitor(cout);

    // // create AST
    // cypher::Compiler compiler;
    // auto tree = compiler.syntax_tree("MATCH (n) DELETE n");

    // // traverser the tree
    // tree.root->accept(*print_visitor);
    //
    assert(calc(0) == 1);

    return 0;
}
