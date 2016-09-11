#include "profile.hpp"

#include "barrier/barrier.cpp"

#include "database/db.hpp"
#include "database/db_accessor.hpp"

#include <chrono>
#include <ctime>
#include <strings.h>
#include <unistd.h>
#include <unordered_map>

#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "import/csv_import.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/command_line/arguments.hpp"

using namespace std;

// Accepts flags for csv import.
// -db name # will create database with that name.
// -s true # will create snapshot of the database after import.
int main(int argc, char **argv)
{
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());

    auto para = all_arguments(argc, argv);
    Db db(get_argument(para, "-db", "powerlinks_profile"));

    import_csv_from_arguments(db, para);

    {
        DbAccessor t(db);

        vector<pair<barrier::VertexAccessor, unordered_map<string, double>>>
            coll;

        // QUERY BENCHMARK
        auto begin = clock();
        int n = for_all_companys(barrier::trans(t), coll);
        clock_t end = clock();
        double elapsed_s = (double(end - begin) / CLOCKS_PER_SEC);

        if (n == 0) {
            cout << "No companys" << endl;
            return 0;
        }

        cout << endl
             << "Query duration: " << (elapsed_s / n) * 1000 * 1000 << " [us]"
             << endl;
        cout << "Throughput: " << 1 / (elapsed_s / n) << " [query/sec]" << endl;

        auto res = coll.back();
        while (res.second.empty()) {
            coll.pop_back();
            res = coll.back();
        }

        auto prop_vertex_id = t.vertex_property_key<Int64>("company_id");
        cout << endl
             << "Example: "
             << *barrier::trans(res.first).at(prop_vertex_id).get() << endl;
        for (auto e : res.second) {
            cout << e.first << " = " << e.second << endl;
        }

        double sum = 0;
        for (auto r : coll) {
            for (auto e : r.second) {
                sum += e.second;
            }
        }

        cout << endl << endl << "Compiler sum " << sum << endl;
        t.commit();
    }

    if (get_argument(para, "-s", "false") == "true") {
        db.snap_engine.make_snapshot();
    }
    // usleep(1000 * 1000 * 60);

    return 0;
}
