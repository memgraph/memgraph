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

    void visit(ast::InternalIdExpr& internal_id) override
    {
    }

    void visit(ast::Return& ret) override
    {
#ifdef DEBUG
        // TODO: remove from here
        code += line("PRINT_PROPS(vertex_accessor.properties());");
        code += line("cout << \"LABELS:\" << endl;");
        code += line("for (auto label_ref : vertex_accessor.labels()) {");
        code += line("cout << label_ref.get() << endl;");
        code += line("}");
#endif

        code += line("t.commit();");
        code += line("return std::make_shared<QueryResult>();");
    }
};
