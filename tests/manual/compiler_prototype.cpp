#include <iostream>
#include <sstream>

#include "dbms/dbms.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.cpp"
#include "query/interpreter.hpp"
#include "query/backend/cpp/typed_value.hpp"
#include "query/frontend/logical/operator.hpp"

using std::cout;
using std::cin;
using std::endl;

/**
 * A Stream implementation that writes out to the
 * console (for testing and debugging only).
 */
// TODO move somewhere to /test/manual or so
class ConsoleResultStream : public Loggable {
public:
  ConsoleResultStream() : Loggable("ConsoleResultStream") {}

  void Header(const std::vector<std::string> &) { logger.info("header"); }

  void Result(std::vector<TypedValue> &values) {
    std::stringstream ss;
    bool first = true;
    for (auto value : values) {
      if (first) {
        ss << "\t";
        first = false;
      }
      else
        ss << " | ";
      switch (value.type()) {
        case TypedValue::Type::Vertex: {
          auto va = value.Value<VertexAccessor>();
          ss << "Vertex(";
          for (auto label : va.labels())
            ss << ":" << va.db_accessor().label_name(label) << " ";
          ss << "{";
          for (auto kv : va.Properties()) {
            ss << va.db_accessor().property_name(kv.first) << ": ";
            ss << kv.second << ", ";
          }
          ss << "}";
          ss << ")";
          break;
        }
        case TypedValue::Type::Edge: {
          auto ea = value.Value<EdgeAccessor>();
          ss << "Edge[" << ea.db_accessor().edge_type_name(ea.edge_type()) << "}";
          ss << "{";
          for (auto kv : ea.Properties()) {
            ss << ea.db_accessor().property_name(kv.first) << ": ";
            ss << kv.second << ", ";
          }
          ss << "}";
          ss << "]";
          break;
        }

        case TypedValue::Type::List:break;
        case TypedValue::Type::Map:break;
        case TypedValue::Type::Path:break;
        default:
          ss << value;
      }
    }
    logger.info("{}", ss.str());
  }

  void Summary(const std::map<std::string, TypedValue> &summary) {
    std::stringstream ss;
    ss << "Summary {";
    bool first = true;
    for (auto kv : summary)
      ss << kv.first << " : " << kv.second << ", ";
    ss << "}";
    logger.info("{}", ss.str());
  }
};


int main(int argc, char* argv[]) {
  // init arguments
  REGISTER_ARGS(argc, argv);

  // init logger
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());

  // init db context
  Dbms dbms;
  ConsoleResultStream stream;

  // initialize the database
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

  cout << "-- Memgraph Query Engine --" << endl;

  while (true) {
    auto inner_dba = dbms.active();
    // read command
    cout << "> ";
    std::string command;
    std::getline(cin, command);
    if (command == "quit") break;
    // execute command / query
    // try {
      // auto db_accessor = dbms.active();
      query::Interpret(command, *inner_dba, stream);
    inner_dba->commit();
    // } catch (const std::exception& e) {
    //   cout << e.what() << endl;
    // } catch (...) {
    //   // pass
    // }
//
  }

  return 0;
}
