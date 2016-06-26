#pragma once

#include "code.hpp"
#include "cypher/visitor/traverser.hpp"
#include "query_engine/util.hpp"

class ReadTraverser : public Traverser, public Code
{
private:
    uint32_t index{0};

public:
    std::string code;

    void visit(ast::Match& match) override
    {
        code += line("auto& t = db.tx_engine.begin();");

        Traverser::visit(match);
    };

    void visit(ast::Property& property) override
    {
        code += line("auto id = args[" + std::to_string(index) + "]->as<Int32>();");
        code += line("auto vertex_accessor = db.graph.vertices.find(t, id.value);");

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
