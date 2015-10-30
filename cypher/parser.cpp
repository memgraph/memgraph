#include <cstdlib>
#include <vector>
#include <vector>

#include "compiler.hpp"
#include "debug/tree_print.hpp"
#include "codegen/cppgen.hpp"

using std::cout;
using std::endl;
using vector_str = std::vector<std::string>;

// TODO: extract from here
// TODO: write more safe and optimal
vector_str all_arguments(int argc, char *argv[])
{
    vector_str args(argv + 1, argv + argc);
    return args;
}

std::string get_argument(const vector_str& all,
                         const std::string& flag,
                         const std::string& default_value)
{
    auto it = std::find(all.begin(), all.end(), flag);
    if (it == all.end()) {
        return default_value;
    }
    auto pos = std::distance(all.begin(), it);
    return all[pos + 1];
}

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
    auto traverser = get_argument(arguments, "-t", "print");

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

