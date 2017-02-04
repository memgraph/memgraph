#include "database/graph_db.hpp"
#include "import/csv_import.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/command_line/arguments.hpp"

using namespace std;

// Tool for importing csv to make snapshot of the database after import.
// Accepts flags for csv import.
// -db name # will create database with that name.
int main(int argc, char **argv)
{
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());

    auto para = all_arguments(argc, argv);
    Db db(get_argument(para, "-db", "default"));

    import_csv_from_arguments(db, para);

    db.snap_engine.make_snapshot();

    return 0;
}
