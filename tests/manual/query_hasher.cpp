#include <iostream>

#include "query/language/cypher/common.hpp"
#include "query/preprocesor.hpp"
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

    QueryPreprocessor preprocessor;
    auto preprocessed = preprocessor.preprocess(input_query);

    cout << "QUERY: " << input_query << endl;
    cout << "STRIPPED QUERY: " << preprocessed.query << endl;
    cout << "QUERY HASH: " << preprocessed.hash << endl;
    cout << "PROPERTIES:" << endl;
    for (auto property : preprocessed.arguments) {
        cout << "    " << property << endl;
    }

    return 0;
}
