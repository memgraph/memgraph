#include <iostream>
#include <cassert>

#include "cypher/compiler.hpp"
#include "cypher/debug/tree_print.hpp"

#include <fstream>
#include <iostream>
#include <iterator>
#include <iterator>
#include <vector>
#include <experimental/filesystem>

namespace fs = std::experimental::filesystem;

using std::cout;
using std::endl;

std::vector<std::string> load_queries()
{
    std::vector<std::string> queries;
    fs::path queries_path = "data/cypher_queries";
    for (auto& directory_entry :
            fs::recursive_directory_iterator(queries_path)) {
        if (!fs::is_regular_file(directory_entry))
            continue;
		std::ifstream infile(directory_entry.path().c_str());
		if (infile) {
			std::string file_text((std::istreambuf_iterator<char>(infile)),
                                  std::istreambuf_iterator<char>());
            queries.emplace_back(file_text);
        }
    }
    return queries;
}

int main()
{
    auto queries = load_queries();

    for (auto& query : queries) {
        auto print_visitor = new PrintVisitor(cout);
        cypher::Compiler compiler;
        auto tree = compiler.syntax_tree(query);
        tree.root->accept(*print_visitor);
    } 

    return 0;
}
