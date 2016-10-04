#include <chrono>
#include <ctime>
#include <strings.h>
#include <unistd.h>
#include <unordered_map>

#include "barrier/barrier.cpp"
#include "database/db.hpp"
#include "database/db_accessor.hpp"
#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "import/csv_import.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/command_line/arguments.hpp"
#include "profile.hpp"

using namespace std;

// (company, {type_name, score})
using company_profile_type =
    pair<barrier::VertexAccessor, unordered_map<string, double>>;

// Accepted flags for CSV import.
// -db name # will create database with that name.
// -s true  # will create snapshot of the database after import.
int main(int argc, char **argv)
{
    // initialize logger
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());

    // read program arguments
    auto para = all_arguments(argc, argv);
    Db db(get_argument(para, "-db", "powerlinks_profile"));

    // import database
    import_csv_from_arguments(db, para);

    {
        DbAccessor t(db);
        vector<company_profile_type> company_profiles;

        // query benchmark
        auto begin = clock();
        int n = for_all_companys(barrier::trans(t), company_profiles);
        clock_t end = clock();
        double elapsed_s = (double(end - begin) / CLOCKS_PER_SEC);

        if (n == 0) {
            cout << "No companys" << endl;
            return 0;
        }

        // performance statistics
        cout << endl
             << "Query duration: " << (elapsed_s / n) * 1000 * 1000 << " [us]"
             << endl;
        cout << "Throughput: " << 1 / (elapsed_s / n) << " [query/sec]" << endl;

        // remove ones who don't have profile results
        auto res = company_profiles.back();
        while (res.second.empty()) {
            company_profiles.pop_back();
            res = company_profiles.back();
        }

        // print specific company
        int company_id = std::stoi(get_argument(para, "-company_id", "230216"));
        for (auto &company_profile : company_profiles) {
            auto prop_vertex_id = t.vertex_property_key<Int64>("company_id");
            auto db_company_id =
                *barrier::trans(company_profile.first).at(prop_vertex_id).get();
            if (db_company_id == company_id) {
                cout << endl << "CompanyID: " << company_id << endl;
                for (auto e : company_profile.second) {
                    cout << e.first << " = " << e.second << endl;
                }
            }
        }

        t.commit();
    }

    if (get_argument(para, "-s", "false") == "true") {
        db.snap_engine.make_snapshot();
    }

    // usleep(1000 * 1000 * 60);

    return 0;
}
