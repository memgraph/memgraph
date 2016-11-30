#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "database/db.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "_hardcoded_query/basic.hpp"
#include "_hardcoded_query/dressipi.hpp"
#include "query/strip/stripper.hpp"
#include "utils/string/file.hpp"
#include "utils/variadic/variadic.hpp"
#include "utils/command_line/arguments.hpp"

Logger logger;

int main(int argc, char *argv[])
{
    auto arguments = all_arguments(argc, argv);

    // POSSIBILITIES: basic, dressipi
    auto suite_name = get_argument(arguments, "-s", "basic");
    // POSSIBILITIES: query_execution, hash_generation
    auto work_mode = get_argument(arguments, "-w", "query_execution");

    // init logging
    logging::init_sync();
    logging::log->pipe(std::make_unique<Stdout>());
    auto log = logging::log->logger("test");

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
        if (query.empty())
            continue;

        utils::println("");
        utils::println("Query: ", query);

        auto stripped = stripper.strip(query);
        utils::println("Hash: ", stripped.hash);
        
        utils::println("------------------------");

        // TODO: more robust solution (enum like)
        if (work_mode == "hash_generation") continue;

        auto result =
            query_functions[stripped.hash](std::move(stripped.arguments));
        permanent_assert(result == true,
                         "Result retured from query function is not true");
        utils::println("------------------------");
    }

    return 0;
}
