#pragma once

#include <iostream>
#include <map>
#include <typeinfo>
#include <fmt/format.h>

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
        code += LINE(code::transaction_begin);
        code += LINE(code::vertex_accessor);

        Traverser::visit(create);
    };

    void visit(ast::LabelList &label_list) override
    {
        // TODO: dummy approach -> discussion
        if (!collecting_labels) collecting_labels = true;

        Traverser::visit(label_list);

        if (collecting_labels) {
            for (auto label : labels) {
                code += LINE(fmt::format(code::create_label, label, label));
                code += LINE(fmt::format(code::add_label, label));
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
        code += LINE(fmt::format(code::vertex_set_property, key, index));
        ++index;
    }

    void visit(ast::Return &ret) override
    {
        code += LINE(code::transaction_commit);
        code += LINE(code::return_empty_result);
    }
};
