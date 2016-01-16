#include <cstdlib>
#include <vector>
#include <vector>

#include "compiler.hpp"
#include "debug/tree_print.hpp"
#include "codegen/cppgen.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/string/filereader.hpp"

using std::cout;
using std::endl;

// * QUERY EXAMPLES *
// "CREATE (n { name: 'Dominik', age: 24, role: 'CEO' }) return n"
// "MATCH (user:User { name: 'Dominik', age: 8 + 4})-[has:HAS|IS|CAN { duration: 'PERMANENT'}]->(item:Item)--(shop)"
// "MATCH (user:User { name: 'Dominik', age: 24})-[has:HAS]->(item:Item) WHERE item.name = 'XPS 13' AND item.price = 11999.99 RETURN user, has, item"

// * INPUT ARGUMENTS *
// -q -> query
// -v -> visitor
// -f -> file

std::string extract_query(const vector_str& arguments)
{
    if (contain_argument(arguments, "-q"))
        return get_argument(arguments, "-q", "CREATE (n {a:1, b:2}) RETURN n");
    auto default_file = "query/read/match/match-where.cypher";
    auto file = get_argument(arguments, "-f", default_file);
    // TODO: error handling
    return read_file(file.c_str());
}

int main(int argc, char *argv[])
{
    // arguments parsing
    auto arguments = all_arguments(argc, argv);

    // query extraction
    auto cypher_query = extract_query(arguments);

    // traversers
    auto traverser = get_argument(arguments, "-t", "code");
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

