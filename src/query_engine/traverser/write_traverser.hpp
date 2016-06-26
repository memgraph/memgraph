#pragma once

#include <iostream>
#include <typeinfo>
#include <map>

#include "cypher/visitor/traverser.hpp"
#include "query_engine/util.hpp"


class WriteTraverser : public Traverser
{
private:
    uint32_t index{0};

public:
    std::string code;

    void visit(ast::Create& create) override
    {
        code += line("auto& t = db.tx_engine.begin();");
        code += line("auto vertex_accessor = db.graph.vertices.insert(t);");

        Traverser::visit(create);
    };

    void visit(ast::Property& property) override
    {
        auto key = property.idn->name;

        code += line("vertex_accessor.property(");
        code += line("\t\"" + key + "\", args[" + std::to_string(index) + "]");
        code += line(");");

        ++index;
    }

    void visit(ast::Return& ret) override
    {
        code += line("t.commit();");
        // code += line("auto &properties = vertex_accessor.properties();");
        // code += line("ResultList::data_t data = {&properties};");
        // code += line("auto result_data = "
        //              "std::make_shared<ResultList>(std::move(data));");
        // code += line("QueryResult::data_t query_data = {{\"" +
        //              ret.return_list->value->name + "\", result_data}};");
        // code += line("return std::make_shared<QueryResult>"
        //              "(std::move(query_data));");
    }
};
