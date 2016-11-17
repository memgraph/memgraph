#include <cstdlib>
#include <vector>
#include <vector>

#include "query/language/cypher/common.hpp"
#include "query/language/cypher/compiler.hpp"
#include "query/language/cypher/debug/tree_print.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/terminate_handler.hpp"

using std::cout;
using std::endl;

int main(int argc, char *argv[])
{
    std::set_terminate(&terminate_handler);

    // // arguments parsing
    auto arguments = all_arguments(argc, argv);

    // // query extraction
    auto queries = extract_queries(arguments);


    for (auto &query : queries)
    {
        cout << "QUERY: " << query << endl;
        auto print_visitor = new PrintVisitor(cout);
        cypher::Compiler compiler;
        auto tree = compiler.syntax_tree(query);
        tree.root->accept(*print_visitor);
        cout << endl;
    }

    return 0;
}
