#include <iostream>
#include <cstdlib>
#include <vector>

#include "debug/log.hpp"
#include "benchmark.hpp"

void help()
{
    std::cout << "error: too few arguments." << std::endl
              << "usage: host port connections_per_query duration[s]"
              << std::endl;

    std::exit(0);
}

int main(int argc, char* argv[])
{
    if(argc < 5)
        help();

    auto host        = std::string(argv[1]);
    auto port        = std::string(argv[2]);
    auto connections = std::stoi(argv[3]);
    auto duration    = std::stod(argv[4]);

    // neo4j
    std::vector<std::string> queries {
        "CREATE (n:Item{id:@}) RETURN n",
        "MATCH (n:Item{id:#}),(m:Item{id:#}) CREATE (n)-[r:test]->(m) RETURN r",
        "MATCH (n:Item{id:#}) SET n.prop = # RETURN n",
        "MATCH (n:Item{id:#}) RETURN n",
        "MATCH (n:Item{id:#})-[r]->(m) RETURN count(r)"
    };

    auto threads = queries.size();

    std::cout << "Running " << queries.size() << " queries each on "
              << connections << " connections "
              << "using a total of " << connections * threads << " connections "
              << "for " << duration << " seconds." << std::endl
              << "..." << std::endl;

    auto result = benchmark(host, port, connections, duration, queries);

    auto& reqs = result.requests;
    auto elapsed = result.elapsed.count();

    auto total = std::accumulate(reqs.begin(), reqs.end(), 0.0,
        [](auto acc, auto x) { return acc + x; }
    );

    std::cout << "Total of " << total << " requests in "
              << elapsed  << "s (" << int(total / elapsed) << " req/s)."
              << std::endl;

    for(size_t i = 0; i < queries.size(); ++i)
        std::cout << queries[i] << " => "
                  << int(reqs[i] / elapsed) << " req/s." << std::endl;

    return 0;
}
