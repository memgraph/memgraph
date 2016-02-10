#pragma once

#include <iostream>
#include <typeinfo>
#include <map>

#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/jsonwriter.hpp"
#include "cypher/visitor/traverser.hpp"
#include "node_traverser.hpp"

using std::cout;
using std::endl;

class CreateTraverser : public Traverser
{
public:
    std::string code;
    Properties properties;

    void visit(ast::Create& create) override
    {
        code = "\t\tauto& t = db.tx_engine.begin();\n"; 
        code += "\t\tauto vertex_accessor = db.graph.vertices.insert(t);\n";
        Traverser::visit(create);
    };

    void visit(ast::Node& node) override
    {
        Traverser::visit(node);
    }

    void visit(ast::Return& ret) override
    {
        code += "\t\tauto properties = vertex_accessor.properties();\n";
        code += "\t\tStringBuffer buffer;\n";
        code += "\t\tJsonWriter<StringBuffer> writer(buffer);\n";
        code += "\t\tproperties.accept(writer);\n";
        code += "\t\treturn std::make_shared<QueryResult>(buffer.str());\n";
    }
};
