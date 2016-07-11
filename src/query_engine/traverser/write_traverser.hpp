#pragma once

#include <iostream>
#include <map>
#include <typeinfo>

#include "cypher/visitor/traverser.hpp"
#include "query_engine/util.hpp"

class WriteTraverser : public Traverser
{
private:
    uint32_t index{0};
    std::vector<std::string> labels;
    bool collecting_labels{false};

public:
    std::string code;

    void visit(ast::Create &create) override
    {
        code += line("auto& t = db.tx_engine.begin();");
        code += line("auto vertex_accessor = db.graph.vertices.insert(t);");

        Traverser::visit(create);
    };

    void visit(ast::LabelList &label_list) override
    {
        // TODO: dummy approach -> discussion
        if (!collecting_labels) collecting_labels = true;

        Traverser::visit(label_list);

        if (collecting_labels) {
            for (auto label : labels) {
                code += line("auto &" + label +
                             " = db.graph.label_store.find_or_create(\"" +
                             label + "\");");
                code += line("vertex_accessor.add_label(" + label + ");");
            }
            labels.clear();
            collecting_labels = false;
        }
    }

    void visit(ast::Identifier &identifier) override
    {
        if (collecting_labels) labels.push_back(identifier.name);
    }

    void visit(ast::Property &property) override
    {
        auto key = property.idn->name;

        code += line("vertex_accessor.property(");
        code += line("\t\"" + key + "\", args[" + std::to_string(index) + "]");
        code += line(");");

        ++index;
    }

    void visit(ast::Return &ret) override
    {
#ifdef DEBUG
        // TODO: remove from here
        code += line("PRINT_PROPS(vertex_accessor.properties());");
#endif

        code += line("t.commit();");
        code += line("return std::make_shared<QueryResult>();");
    }
};
