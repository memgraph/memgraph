#include <iostream>

#include "dbms/dbms.hpp"
#include "query/console.hpp"
#include "query/interpreter.hpp"
#include "utils/random_graph_generator.hpp"

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

void fill_db(Dbms &dbms) {
  auto dba = dbms.active();

  // labels
  auto company = dba->label("Company");
  auto person = dba->label("Person");
  auto device = dba->label("Device");

  // props
  auto name = dba->property("name");
  auto age = dba->property("age");
  auto type = dba->property("type");

  // vertices
  auto memgraph = dba->insert_vertex();
  memgraph.PropsSet(name, "Memgraph");
  memgraph.add_label(company);
  auto teon = dba->insert_vertex();
  teon.PropsSet(name, "Teon");
  teon.PropsSet(age, 26);
  teon.add_label(person);
  auto mislav = dba->insert_vertex();
  mislav.PropsSet(name, "Mislav");
  mislav.PropsSet(age, 22);
  mislav.add_label(person);
  auto florijan = dba->insert_vertex();
  florijan.PropsSet(name, "Florijan");
  florijan.PropsSet(age, 31);
  florijan.add_label(person);
  auto xps_15 = dba->insert_vertex();
  xps_15.PropsSet(type, "PC");
  xps_15.PropsSet(name, "Dell XPS 15");
  xps_15.add_label(device);

  // edges
  dba->insert_edge(teon, memgraph, dba->edge_type("MEMBER_OF"));
  dba->insert_edge(mislav, memgraph, dba->edge_type("MEMBER_OF"));
  dba->insert_edge(florijan, memgraph, dba->edge_type("MEMBER_OF"));

  dba->insert_edge(teon, mislav, dba->edge_type("FRIEND_OF"));
  dba->insert_edge(mislav, teon, dba->edge_type("FRIEND_OF"));
  dba->insert_edge(florijan, mislav, dba->edge_type("FRIEND_OF"));
  dba->insert_edge(mislav, florijan, dba->edge_type("FRIEND_OF"));
  dba->insert_edge(florijan, teon, dba->edge_type("FRIEND_OF"));
  dba->insert_edge(teon, florijan, dba->edge_type("FRIEND_OF"));

  dba->insert_edge(memgraph, xps_15, dba->edge_type("OWNS"));

  dba->insert_edge(teon, xps_15, dba->edge_type("USES"));
  dba->insert_edge(mislav, xps_15, dba->edge_type("USES"));
  dba->insert_edge(florijan, xps_15, dba->edge_type("USES"));

  dba->commit();
}

int main(int argc, char *argv[]) {
  REGISTER_ARGS(argc, argv);

  // parse the first cmd line argument as the count of nodes to randomly create
  uint node_count = 100000;
  if (argc > 1) {
    node_count = (uint) std::stoul(argv[1]);
    permanent_assert(node_count < 10000000,
                     "More then 10M nodes requested, that's too much");
  }

  Dbms dbms;
  std::cout << "Generating graph..." << std::endl;
  //  fill_db(dbms);
  random_generate(dbms, node_count);
  query::Repl(dbms);
  return 0;
}
