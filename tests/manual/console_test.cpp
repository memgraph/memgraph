#include <iostream>

#include "dbms/dbms.hpp"
#include "query/console.hpp"
#include "query/interpreter.hpp"
#include "utils/random_graph_generator.hpp"

#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"

void random_generate(Dbms &dbms, uint node_count, int edge_factor = 5) {
  auto dba = dbms.active();
  utils::RandomGraphGenerator generator(*dba);

  auto edge_count = node_count * edge_factor;
  generator.AddVertices(node_count, {"Person"});
  generator.AddEdges(edge_count, "Friend");
  generator.SetVertexProperty<int>(node_count, "age",
                                   utils::RandomIntGenerator(3, 60));
  generator.SetVertexProperty<int>(node_count, "height",
                                   utils::RandomIntGenerator(120, 200));

  generator.Commit();
}

int main(int argc, char *argv[]) {
  REGISTER_ARGS(argc, argv);

  // parse the first cmd line argument as the count of nodes to randomly create
  uint node_count = 0;
  if (argc > 1) {
    node_count = (uint) std::stoul(argv[1]);
    permanent_assert(node_count < 10000000,
                     "More then 10M nodes requested, that's too much");
  }

  // TODO switch to GFlags, once finally available
  if (argc > 2) {
    logging::init_sync();
    logging::log->pipe(std::make_unique<Stdout>());
  }

  Dbms dbms;
  std::cout << "Generating graph..." << std::endl;
  //  fill_db(dbms);
  random_generate(dbms, node_count);
  query::Repl(dbms);
  return 0;
}
