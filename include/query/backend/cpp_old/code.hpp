#pragma once

// !! DEPRICATED !!

#include <string>

#include "query/util.hpp"

class Code
{
public:
    std::string code;

    void reset() { code = ""; }
};

namespace code
{

// TODO: one more abstraction level
// TODO: UNIT tests

const std::string transaction_begin = "DbAccessor t(db);";

const std::string transaction_commit = "t.commit();";

const std::string set_property = "{}.set({}, std::move(args[{}]));";

// create vertex e.g. CREATE (n:PERSON {name: "Test", age: 23})
const std::string create_vertex = "auto {} = t.vertex_insert();";
const std::string create_label = "auto &{0} = t.label_find_or_create(\"{0}\");";
const std::string add_label = "{}.add_label({});";

const std::string vertex_property_key =
    "auto {}=t.vertex_property_key(\"{}\",args[{}].key.flags());";
const std::string edge_property_key =
    "auto {}=t.edge_property_key(\"{}\",args[{}].key.flags());";

// create edge e.g CREATE (n1)-[r:COST {cost: 100}]->(n2)
const std::string create_edge = "auto {} = t.edge_insert({},{});";
const std::string find_type = "auto &{0} = t.type_find_or_create(\"{0}\");";
const std::string set_type = "{}.edge_type({});";

const std::string args_id = "Int32 id = args[{}].as<Int32>();";

const std::string vertex_accessor_args_id =
    "auto vertex_accessor = t.vertex_find(id.value());";

const std::string match_vertex_by_id =
    "auto option_{0} = t.vertex_find(args[{1}].as<Int64>().value());\n"
    "        if (!option_fill(option_{0})) return t.commit(), false;\n"
    "        auto {0}=option_{0}.take();";
const std::string match_edge_by_id =
    "auto option_{0} = t.edge_find(args[{1}].as<Int64>().value());\n"
    "        if (!option_fill(option_{0})) return t.commit(), false;\n"
    "        auto {0}=option_{0}.take();";

const std::string write_entity = "stream.write_field(\"{0}\");\n"
                                 "        stream.write_record();\n"
                                 "        stream.write_list_header(1);\n"
                                 "        stream.write({0});\n"
                                 "        stream.chunk();"
                                 "        stream.write_meta(\"rw\");\n";

const std::string write_vertex_accessor =
    "        stream.write_record();\n"
    "        stream.write_list_header(1);\n"
    "        stream.write(vertex_accessor);\n"
    "        stream.chunk();\n";

const std::string write_all_vertices =
    "stream.write_field(\"{0}\");\n"
    "        iter::for_all(t.vertex_access(), [&](auto vertex_accessor) {{\n"
    "            if (vertex_accessor.fill()) {{\n"
    + write_vertex_accessor +
    "            }}\n"
    "        }});\n"
    "        stream.write_meta(\"rw\");\n";

const std::string find_and_write_vertices_by_label =
    "auto &label = t.label_find_or_create(\"{1}\");\n"
    "        stream.write_field(\"{0}\");\n"
    "        label.index().for_range(t).for_all([&](auto vertex_accessor) {{\n"
    + write_vertex_accessor +
    "        }});\n"
    "        stream.write_meta(\"rw\");\n";

const std::string find_and_write_vertices_by_label_and_properties =
    "{{\n"
    "    DbAccessor _t(db);\n"
    "    {0}\n"
    "    auto properties = query_properties(indices, args);\n"
    "    auto &label = _t.label_find_or_create(\"{1}\");\n"
    "    stream.write_field(\"{2}\");\n"
    "    label.index().for_range(_t).properties_filter(_t, properties).for_all(\n"
    "    [&](auto vertex_accessor) -> void {{\n"
    "        "+ write_vertex_accessor +
    "    }});\n"
    "    stream.write_meta(\"rw\");\n"
    "    _t.commit();"
    "}}\n";

// -- LABELS
const std::string set_vertex_element =
    "{{\n"
    "    DbAccessor _t(db);" // TODO: HACK (set labels should somehow persist the state)
    "    {0}\n"
    "    auto properties = query_properties(indices, args);\n"
    "    auto &label = _t.label_find_or_create(\"{1}\");\n"
    "    label.index().for_range(_t).properties_filter(_t, properties).for_all(\n"
    "        [&](auto vertex_accessor) -> void {{\n"
    "            auto {2} = _t.vertex_property_key(\"{2}\", args[{3}].key.flags());\n"
    "            vertex_accessor.set({2}, std::move(args[{3}]));\n"
    "        }}\n"
    "    );\n"
    "    _t.commit();\n"
    "}}";

const std::string set_labels_start =
    "        {{\n"
    "            DbAccessor _t(db);" // TODO: HACK (set labels should somehow persist the state)
    "            {0}\n"
    "            auto properties = query_properties(indices, args);\n"
    "            auto &label = _t.label_find_or_create(\"{1}\");\n"
    "            label.index().for_range(_t).properties_filter(_t, properties).for_all(\n"
    "                [&](auto vertex_accessor) -> void {{\n";
const std::string set_label =
    "                    auto &{0} = _t.label_find_or_create(\"{0}\");\n"
    "                    vertex_accessor.add_label({0});\n";
const std::string set_labels_end =
    "                }}\n"
    "            );\n"
    "            _t.commit();"
    "        }}";

const std::string return_labels =
    "{{\n"
    "     DbAccessor _t(db);" // TODO: HACK (set labels should somehow persist the state)
    "     {0}\n"
    "     auto properties = query_properties(indices, args);\n"
    "     auto &label = _t.label_find_or_create(\"{1}\");\n"
    "     stream.write_field(\"labels({2})\");\n"
    "     label.index().for_range(_t).properties_filter(_t, properties).for_all(\n"
    "         [&](auto vertex_accessor) -> void {{\n"
    "             auto &labels = vertex_accessor.labels();\n"
    "             stream.write_record();\n"
    "             stream.write_list_header(1);\n" // TODO: figure out why
    "             stream.write_list_header(labels.size());\n"
    "             for (auto &label : labels) {{\n"
    "                 stream.write(label.get().str());\n"
    "             }}\n"
    "             stream.chunk();\n"
    "         }}\n"
    "     );\n"
    "     stream.write_meta(\"rw\");\n"
    "     _t.commit();\n"
    "}}";

const std::string write_all_edges =
    "stream.write_field(\"{0}\");\n"
    "        iter::for_all(t.edge_access(), [&](auto edge) {{\n"
    "            if (edge.fill()) {{\n"
    "                stream.write_record();\n"
    "                stream.write_list_header(1);\n"
    "                stream.write(edge);\n"
    "                stream.chunk();\n"
    "            }}\n"
    "        }});\n"
    "        stream.write_meta(\"rw\");\n";

const std::string find_and_write_edges_by_type =
    "auto &type = t.type_find_or_create(\"{1}\");\n"
    "        stream.write_field(\"{0}\");\n"
    "        type.index().for_range(t).for_all([&](auto edge) {{\n"
    "            stream.write_record();\n"
    "            stream.write_list_header(1);\n"
    "            stream.write(edge);\n"
    "            stream.chunk();\n"
    "        }});\n"
    "        stream.write_meta(\"rw\");\n";

const std::string count_vertices_for_one_label =
    "size_t count = 0;\n"
    "auto &label = t.label_find_or_create(\"{1}\");\n"
    "        label.index().for_range(t).for_all([&](auto vertex) {{\n"
    "            count++;\n"
    "        }});\n"
    "        stream.write_field(\"count({0})\");\n"
    "        stream.write_record();\n"
    "        stream.write_list_header(1);\n"
    "        stream.write(Int64(count));\n"
    "        stream.chunk();\n"
    "        stream.write_meta(\"r\");\n";

// TODO: vertices and edges
const std::string count =
    "size_t count = 0;\n"
    "        t.vertex_access().fill().for_all(\n"
    "            [&](auto vertex) {{ ++count; }});\n"
    "        stream.write_field(\"count({0})\");\n"
    "        stream.write_record();\n"
    "        stream.write_list_header(1);\n"
    "        stream.write(Int64(count));\n"
    "        stream.chunk();\n"
    "        stream.write_meta(\"r\");\n";

const std::string return_true = "return true;";

const std::string todo = "// TODO: {}";
const std::string print_properties =
    "cout << \"{0}\" << endl;\n"
    "        cout_properties({0}.properties());";
const std::string print_property =
    "cout_property(\"{0}\", {0}.property(\"{1}\"));";
}

// DELETE
const std::string delete_all_detached_nodes =
    "t.vertex_access().fill().isolated().for_all(\n"
    "        [&](auto a) {{ a.remove(); }});\n"
    "        stream.write_empty_fields();\n"
    "        stream.write_meta(\"w\");\n";
const std::string delete_whole_graph = 
    "t.edge_access().fill().for_all(\n"
    "        [&](auto e) { e.remove(); }\n"
    ");\n" + delete_all_detached_nodes;
