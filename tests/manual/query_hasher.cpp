#include <iostream>

#include "cypher/common.hpp"
#include "query_engine/query_hasher.hpp"
#include "query_engine/query_stripper.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/type_discovery.hpp"

using std::cout;
using std::cin;
using std::endl;

int main(int argc, char **argv)
{
    // arguments parsing
    auto arguments = all_arguments(argc, argv);

    // query extraction
    auto input_query = extract_query(arguments);

    cout << "QUERY: " << input_query << endl;

    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);
    auto stripped = stripper.strip(input_query);

    cout << "STRIPPED QUERY: " << stripped.query << endl;

    QueryHasher query_hasher;

    cout << "QUERY HASH: " << query_hasher.hash(stripped.query) << endl;

    cout << "PROPERTIES:" << endl;
    for (auto property : stripped.arguments) {
        cout << "    " << property << endl;
    }

    return 0;
}
