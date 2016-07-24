#pragma once

// TODO: wrap fmt format
#include <fmt/format.h>

#include "code.hpp"
#include "cypher/visitor/traverser.hpp"
#include "query_engine/util.hpp"
#include "utils/underlying_cast.hpp"
#include "query_engine/code_generator/entity_search.hpp"

using namespace entity_search;

class ReadTraverser : public Traverser, public Code
{
private:
    // TODO: change int to something bigger (int64_t)
    std::map<std::string, int> internal_ids;

    std::set<std::string> entities;
    CypherStateMachine csm;
    uint32_t index{0};
    std::map<std::string, uint32_t> property_indices;

public:
    void update_entities(const std::string &name)
    {
        std::cout << name << std::endl;
        if (entities.find(name) == entities.end()) {
            csm.init_cost(name);
            property_indices[name] = index++;
            entities.insert(std::move(name));
        }
    }

    void visit(ast::Match &match) override
    {
        code += LINE(code::transaction_begin);
        Traverser::visit(match);
    };

    void visit(ast::Node &node) override
    {
        auto identifier = node.idn;
        if (identifier == nullptr) return;

        auto name = identifier->name;
        update_entities(name);

        if (node.labels != nullptr && node.labels->value != nullptr)
            csm.search_cost(name, search_label_index, label_cost);
    }

    void visit(ast::InternalIdExpr &internal_id_expr) override
    {
        if (internal_id_expr.identifier == nullptr) return;
        auto name = internal_id_expr.identifier->name;

        // value has to exist
        if (internal_id_expr.value == nullptr) {
            // TODO: raise exception
        }

        update_entities(name);
        csm.search_cost(name, search_internal_id, internal_id_cost);
    }

    void visit(ast::Return &ret) override
    {
        for (auto const &entity : entities) {
            std::cout << entity << std::endl;
            std::cout << underlying_cast(csm.min(entity)) << std::endl;
            if (csm.min(entity) == search_internal_id) {
                auto property_index = property_indices.at(entity);
                code += LINE(
                    fmt::format(code::args_id, std::to_string(property_index)));
                code += LINE(code::vertex_accessor_args_id);
                continue;
            }

            if (csm.min(entity) == search_label_index) {
                code += LINE(fmt::format(code::todo, "search label index"));
                continue;
            }

            if (csm.min(entity) == search_main_storage) {
                code += LINE(fmt::format(code::todo, "return all vertices"));
                continue;
            }
        }

        code::debug_print_vertex_labels();

        code += LINE(code::transaction_commit);
        code += LINE(code::return_empty_result);
    }
};
