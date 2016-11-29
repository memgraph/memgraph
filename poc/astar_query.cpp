#include <chrono>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <queue>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "communication/bolt/v1/serialization/bolt_serializer.hpp"
#include "data_structures/map/rh_hashmap.hpp"
#include "database/db.hpp"
#include "database/db_accessor.cpp"
#include "database/db_accessor.hpp"
#include "import/csv_import.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "queries/astar.hpp"
#include "storage/edge_x_vertex.hpp"
#include "storage/edges.cpp"
#include "storage/edges.hpp"
#include "storage/indexes/impl/nonunique_unordered_index.cpp"
#include "storage/model/properties/properties.cpp"
#include "storage/record_accessor.cpp"
#include "storage/vertex_accessor.hpp"
#include "storage/vertices.cpp"
#include "storage/vertices.hpp"
#include "utils/command_line/arguments.hpp"

int main(int argc, char **argv)
{
    logging::init_async();
    logging::log->pipe(std::make_unique<Stdout>());
    std::srand(time(0));

    auto para = all_arguments(argc, argv);

    Db db("astar");
    PlanCPU plan;
    int bench_n = 1000;

    do
    {
        double sum = 0;
        for (int i = 0; i < bench_n; i++)
        {
            auto start_vertex_index =
                std::rand() % db.graph.vertices.access().size();

            auto begin = clock();

            plan_args_t args;
            args.push_back(Property(Int64(start_vertex_index), Int64::type));

            plan.run(db, args, std::cout);

            clock_t end = clock();

            double elapsed_ms = (double(end - begin) / CLOCKS_PER_SEC) * 1000;
            sum += elapsed_ms;
        }

        std::cout << "\nSearch for best " << limit
                  << " results has runing time of:\n    avg: " << sum / bench_n
                  << " [ms]\n";
    } while (true);

    return 0;
}
