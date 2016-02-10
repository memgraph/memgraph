#include <cstdlib>
#include <vector>
#include <vector>

#include "compiler.hpp"
#include "debug/tree_print.hpp"
#include "utils/command_line/arguments.hpp"
#include "cypher/common.hpp"
#include "utils/terminate_handler.hpp"

// dumping ground for all kind of visitors
#include "query_engine/traverser/create_traverser.hpp"

using std::cout;
using std::endl;

// * INPUT ARGUMENTS *
// -q -> query
// -v -> visitor
// -f -> file
//
int main(int argc, char *argv[])
{
    std::set_terminate(&terminate_handler);

    // arguments parsing
    auto arguments = all_arguments(argc, argv);

    // query extraction
    auto cypher_query = extract_query(arguments);
    cout << "QUERY: " << cypher_query << endl;

    // traversers
    auto traverser = get_argument(arguments, "-t", "code");
    auto print_traverser = Traverser::sptr(new PrintVisitor(cout));
    auto create_traverser = Traverser::sptr(new CreateTraverser());
    std::map<std::string, Traverser::sptr> traversers = {
        {"print", print_traverser},
        {"create", create_traverser}
    };

    cypher::Compiler compiler;
    auto tree = compiler.syntax_tree(cypher_query);

    auto t = traversers[traverser];
    tree.root->accept(*t);

    return 0;
}

