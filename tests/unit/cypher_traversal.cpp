#include <iostream>
#include <cassert>
#include <fstream>
#include <iostream>
#include <iterator>
#include <iterator>
#include <vector>
#include <experimental/filesystem>

#include "query/language/cypher/compiler.hpp"
#include "query/language/cypher/debug/tree_print.hpp"

namespace fs = std::experimental::filesystem;

using std::cout;
using std::endl;

auto load_queries()
{
    std::vector<std::string> queries;

    fs::path queries_path = "data/queries/cypher";
    std::string query_file_extension = "cypher";

    for (auto& directory_entry :
            fs::recursive_directory_iterator(queries_path)) {

        auto path = directory_entry.path().string();

        // skip directories
        if (!fs::is_regular_file(directory_entry))
            continue;

        // skip non cypher files
        auto file_extension = path.substr(path.find_last_of(".") + 1);
        if (file_extension != query_file_extension)
            continue;

        // load a cypher query and put the query into queries container
		std::ifstream infile(path.c_str());
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
    std::string comment = "#";
    
    int counter = 0;
    for (auto& query : queries) {
        if (query.substr(0, comment.size()) == comment) {
            cout << "Query is commented out: " << query << endl;
            continue;
        }
        cout << "QUERY IS: " << query << endl;
        auto print_visitor = new PrintVisitor(cout);
        cypher::Compiler compiler;
        auto tree = compiler.syntax_tree(query);
        tree.root->accept(*print_visitor);
        cout << endl << "Test ok: " << query << endl;
        counter++;
        delete print_visitor;
    } 
    cout << endl << endl << counter << " tests passed";

    return 0;
}
