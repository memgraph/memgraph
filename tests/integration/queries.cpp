#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "database/db.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "query/hardcode/basic.hpp"
#include "query/hardcode/dressipi.hpp"
#include "query/strip/stripper.hpp"
#include "utils/string/file.hpp"
#include "utils/variadic/variadic.hpp"
#include "utils/command_line/arguments.hpp"

int main(int argc, char *argv[])
{
    auto arguments = all_arguments(argc, argv);

    // POSSIBILITIES: basic, dressipi
    auto suite_name = get_argument(arguments, "-s", "basic");
    // POSSIBILITIES: query_execution, hash_generation
    auto work_mode = get_argument(arguments, "-w", "query_execution");

    // init logging
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());

    // init db, functions and stripper
    Db db;
    hardcode::query_functions_t query_functions;
    if (suite_name == "dressipi")
    {
        query_functions = std::move(hardcode::load_dressipi_functions(db));
    }
    else
    {
        query_functions = std::move(hardcode::load_basic_functions(db));
    }
    auto stripper = make_query_stripper(TK_LONG, TK_FLOAT, TK_STR, TK_BOOL);

    // load quries
    std::string file_path = "data/queries/core/" + suite_name + ".txt";
    auto queries          = utils::read_lines(file_path.c_str());

    // execute all queries
    for (auto &query : queries)
    {
        utils::println("Query: ", query);

        auto stripped = stripper.strip(query);
        utils::println("Hash: ", stripped.hash);

        // TODO: more robust solution (enum like)
        if (work_mode == "hash_generation") continue;

        auto result =
            query_functions[stripped.hash](std::move(stripped.arguments));
        permanent_assert(result == true,
                         "Result retured from query function is not true");
        utils::println("------------------------ PASS");
    }

    return 0;
}
