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

// create vertex e.g. CREATE (n:PERSON {name: "Test", age: 23})
const std::string create_vertex =
    "auto {} = db.graph.vertices.insert(t);";
const std::string set_vertex_property =
    "{}.property(\"{}\", args[{}]);";
const std::string create_label =
    "auto &{0} = db.graph.label_store.find_or_create(\"{0}\");";
const std::string add_label = "{}.add_label({});";

const std::string args_id = "auto id = args[{}]->as<Int32>();";

const std::string vertex_accessor_args_id =
    "auto vertex_accessor = db.graph.vertices.find(t, id.value);";

const std::string match_vertex_by_id =
    "auto {0} = db.graph.vertices.find(t, args[{1}]->as<Int64>().value);\n"
    "if (!{0}) return t.commit(), std::make_shared<QueryResult>();";
const std::string match_edge_by_id =
    "auto {0} = db.graph.edges.find(t, args[{1}]->as<Int64>().value);\n"
    "if (!{0}) return t.commit(), std::make_shared<QueryResult>();";

const std::string create_edge =
    "auto {} = db.graph.edges.insert(t);";


const std::string return_empty_result =
    "return std::make_shared<QueryResult>();";

const std::string update_property =
    "{}.property(\"{}\", args[{}]);";


const std::string todo = "// TODO: {}";

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
