#pragma once

#include <string>

#include "query_engine/util.hpp"

struct Code
{
    void reset() { code = ""; }

    std::string code;
};

namespace code
{

// TODO: UNIT tests

const std::string transaction_begin = "auto& t = db.tx_engine.begin();";

const std::string transaction_commit = "t.commit();";

const std::string set_property = "{}.property(\"{}\", args[{}]);";

// create vertex e.g. CREATE (n:PERSON {name: "Test", age: 23})
const std::string create_vertex = "auto {} = db.graph.vertices.insert(t);";
const std::string create_label =
    "auto &{0} = db.graph.label_store.find_or_create(\"{0}\");";
const std::string add_label = "{}.add_label({});";

// create edge e.g CREATE (n1)-[r:COST {cost: 100}]->(n2)
const std::string create_edge = "auto {} = db.graph.edges.insert(t);";
const std::string find_type =
    "auto &{0} = db.graph.edge_type_store.find_or_create(\"{0}\");";
const std::string set_type = "{}.edge_type({});";
const std::string node_out = "{}.vlist->update(t)->data.out.add({}.vlist);";
const std::string node_in = "{}.vlist->update(t)->data.in.add({}.vlist);";
const std::string edge_from = "{}.from({}.vlist);";
const std::string edge_to = "{}.to({}.vlist);";

const std::string args_id = "auto id = args[{}]->as<Int32>();";

const std::string vertex_accessor_args_id =
    "auto vertex_accessor = db.graph.vertices.find(t, id.value);";

const std::string match_vertex_by_id =
    "auto {0} = db.graph.vertices.find(t, args[{1}]->as<Int64>().value);\n"
    "        if (!{0}) return t.commit(), std::make_shared<QueryResult>();";
const std::string match_edge_by_id =
    "auto {0} = db.graph.edges.find(t, args[{1}]->as<Int64>().value);\n"
    "        if (!{0}) return t.commit(), std::make_shared<QueryResult>();";

const std::string return_empty_result =
    "return std::make_shared<QueryResult>();";

const std::string update_property = "{}.property(\"{}\", args[{}]);";

const std::string todo = "// TODO: {}";
const std::string print_properties =
    "cout << \"{0}\" << endl;\n"
    "        cout_properties({0}.properties());";
const std::string print_property =
    "cout_property(\"{0}\", {0}.property(\"{1}\"));";

std::string debug_print_vertex_labels()
{
#ifdef DEBUG
    return LINE("PRINT_PROPS(vertex_accessor.properties());") +
           LINE("cout << \"LABELS:\" << endl;") +
           LINE("for (auto label_ref : vertex_accessor.labels()) {") +
           LINE("cout << label_ref.get() << endl;") + LINE("}");
#else
    return "";
#endif
}
}
