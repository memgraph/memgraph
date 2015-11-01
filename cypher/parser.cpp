#include <cstdlib>
#include <vector>
#include <vector>

#include "compiler.hpp"
#include "debug/tree_print.hpp"
#include "codegen/cppgen.hpp"
#include "utils/command_line/arguments.hpp"

using std::cout;
using std::endl;

// * QUERY EXAMPLES *
// "CREATE (n { name: 'Dominik', age: 24, role: 'CEO' }) return n"
// "MATCH (user:User { name: 'Dominik', age: 8 + 4})-[has:HAS|IS|CAN { duration: 'PERMANENT'}]->(item:Item)--(shop)"
// "MATCH (user:User { name: 'Dominik', age: 24})-[has:HAS]->(item:Item) WHERE item.name = 'XPS 13' AND item.price = 11999.99 RETURN user, has, item"

// * INPUT ARGUMENTS *
// -q -> query
// -v -> visitor

int main(int argc, char *argv[])
{
    // arguments parsing
    auto arguments = all_arguments(argc, argv);
    auto cypher_query = get_argument(arguments, "-q", "CREATE (n {name: 'Domko', age: 24}) return n");
    auto traverser = get_argument(arguments, "-t", "code");

    // traversers
    auto print_traverser = Traverser::sptr(new PrintVisitor(cout));
    auto cppgen_traverser = Traverser::sptr(new CppGen());
    std::map<std::string, Traverser::sptr> traversers = {
        {"print", print_traverser},
        {"code", cppgen_traverser}
    };

    cypher::Compiler compiler;
    auto tree = compiler.syntax_tree(cypher_query);

    auto t = traversers[traverser];
    tree.root->accept(*t);

    return 0;
}

