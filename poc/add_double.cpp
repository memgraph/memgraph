#include <cstring>
#include <ctime>

#include "database/db.hpp"
#include "import/csv_import.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/command_line/arguments.hpp"

using namespace std;

// Adds double property with random value of max to all vertices.
void add_scores(Db &db, double max_value, std::string const &property_name)
{
    DbAccessor t(db);

    auto key_score = t.vertex_property_family_get(property_name)
                         .get(Flags::Double)
                         .family_key();

    std::srand(time(0));
    t.vertex_access().fill().for_all([&](auto v) {
        double value = ((std::rand() + 0.0) / RAND_MAX) * max_value;
        v.set(StoredProperty<TypeGroupVertex>(Double(value), key_score));
    });

    t.commit();
}

int main(int argc, char **argv)
{
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());

    auto para = all_arguments(argc, argv);

    std::string property_name = get_argument(para, "-pn", "score");
    double max_value = std::stod(get_argument(para, "-max", "1"));

    Db db(get_argument(para, "-db", "default"));

    add_scores(db, max_value, property_name);

    db.snap_engine.make_snapshot();

    return 0;
}
