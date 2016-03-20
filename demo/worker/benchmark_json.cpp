#include "benchmark.hpp"

int main(int argc, char* argv[])
{
    if (argc < 6)
        std::exit(EXIT_FAILURE);

    // read arguments
    auto host        = std::string(argv[1]);
    auto port        = std::string(argv[2]);
    auto connections = std::stoi(argv[3]);
    auto duration    = std::stod(argv[4]);
    auto count       = std::stoi(argv[5]);

    counter.store(count);

    std::vector<std::string> queries;
    for (int i = 6; i <  argc; ++i) {
        queries.emplace_back(std::string(argv[i]));
    }

    // print result
    std::cout << benchmark_json(host, port, connections, duration, queries);

    return 0;
}
