#include <cstdlib>
#include <vector>
#include <vector>

#include "cypher/compiler.hpp"
#include "cypher/debug/tree_print.hpp"
#include "utils/command_line/arguments.hpp"
#include "cypher/common.hpp"
#include "utils/terminate_handler.hpp"

using std::cout;
using std::endl;

int main(int argc, char *argv[])
{
    std::set_terminate(&terminate_handler);

    // // arguments parsing
    auto arguments = all_arguments(argc, argv);

    // // query extraction
    auto cypher_query = extract_query(arguments);
    cout << "QUERY: " << cypher_query << endl;

    auto print_visitor = new PrintVisitor(cout);
    cypher::Compiler compiler;
    auto tree = compiler.syntax_tree(cypher_query);
    tree.root->accept(*print_visitor);
    cout << endl;

    return 0;
}
