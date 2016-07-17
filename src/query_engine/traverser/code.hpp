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

const std::string vertex_accessor =
    "auto vertex_accessor = db.graph.vertices.insert(t);";

const std::string args_id = "auto id = args[{}]->as<Int32>();";

const std::string vertex_accessor_args_id =
    "auto vertex_accessor = db.graph.vertices.find(t, id.value);";

const std::string vertex_set_property =
    "vertex_accessor.property(\"{}\", args[{}]);";

const std::string return_empty_result =
    "return std::make_shared<QueryResult>();";

const std::string create_label =
    "auto &{} = db.graph.label_store.find_or_create(\"{}\");";

const std::string add_label = "vertex_accessor.add_label({});";

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
