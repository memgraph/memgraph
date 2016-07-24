#pragma once

#include "code.hpp"
#include "cypher/visitor/traverser.hpp"
#include "query_engine/code_generator/cypher_state.hpp"
#include "query_engine/code_generator/cpp_generator.hpp"
#include "query_engine/code_generator/structures.hpp"

// k
// auto v1 = db.graph.vertices.find(t, args[0]->as<Int32>().value);
// if (!v1) return t.commit(), false;

// auto v2 = db.graph.vertices.find(t, args[1]->as<Int32>().value);
// if (!v2) return t.commit(), false;

// create
// auto edge_accessor = db.graph.edges.insert(t);

// from
// v1.vlist->update(t)->data.out.add(edge_accessor.vlist);
// edge_accessor.from(v1.vlist);

// to
// v2.vlist->update(t)->data.in.add(edge_accessor.vlist);
// edge_accessor.to(v2.vlist);

// edge type
// auto &edge_type = db.graph.edge_type_store.find_or_create("IS");
// edge_accessor.edge_type(edge_type);

class ReadWriteTraverser : public Traverser, public Code
{
private:
    // currently active entity name (node or relationship name)
    std::string entity;

    // currenlty active parameter index
    uint64_t index;

    // currently active state
    CypherState state; // TODO: move to generator
    QueryAction query_action;
    ClauseAction clause_action;

    // linearization
    CppGenerator generator;

    Vector<std::string> visited_nodes;

    RelationshipData::Direction direction;

public:
    void visit(ast::Match &ast_match) override
    {
        state = CypherState::Match;

        generator.add_action(QueryAction::TransactionBegin);
        generator.add_action(QueryAction::Match);

        Traverser::visit(ast_match);
    }

    void visit(ast::Where &ast_where) override
    {
        state = CypherState::Where;

        Traverser::visit(ast_where);
    }

    void visit(ast::Create &ast_create) override
    {
        // generate state before
        generator.generate();

        // save current action
        generator.add_action(QueryAction::Create);

        state = CypherState::Create;
        query_action = QueryAction::Create;

        Traverser::visit(ast_create);
    }

    void visit(ast::Return &ast_return) override
    {
        generator.add_action(QueryAction::Return);
        generator.add_action(QueryAction::TransactionCommit);

        state = CypherState::Return;
        query_action = QueryAction::Return;

        Traverser::visit(ast_return);

        code = generator.generate();
    }

    void visit(ast::PatternList &ast_pattern_list) override
    {
        Traverser::visit(ast_pattern_list);
    }

    void visit(ast::Pattern &ast_pattern) override
    {
        // TODO: Is that traversal order OK for all cases? Probably NOT.
        if (ast_pattern.has_next()) {
            Traverser::visit(*(ast_pattern.next));
            if (ast_pattern.has_node()) Traverser::visit(*(ast_pattern.node));
            if (ast_pattern.has_relationship())
                Traverser::visit(*(ast_pattern.relationship));
        } else {
            Traverser::visit(ast_pattern);
        }
    }

    void visit(ast::Node &ast_node) override
    {
        auto &action_data = generator.action_data();
        auto &cypher_data = generator.cypher_data();

        if (!ast_node.has_identifier()) return;

        auto name = ast_node.idn->name;
        entity = name;
        visited_nodes.push_back(name);

        if (state == CypherState::Match) {
            action_data.actions[name] = ClauseAction::MatchNode;
        }

        if (state == CypherState::Create) {
            if (cypher_data.status(name) == EntityStatus::Matched) {
                action_data.actions[name] = ClauseAction::MatchNode;
            } else {
                action_data.actions[name] = ClauseAction::CreateNode;
            }
        }

        Traverser::visit(ast_node);
    }

    void visit(ast::And &ast_and) override { Traverser::visit(ast_and); }

    void visit(ast::InternalIdExpr &internal_id_expr) override
    {
        if (internal_id_expr.identifier == nullptr) return;
        auto name = internal_id_expr.identifier->name;

        // value has to exist
        if (internal_id_expr.value == nullptr) {
            // TODO: raise exception
        }

        auto &data = generator.action_data();
        data.parameter_index[ParameterIndexKey(InternalId, name)] = index++;
        data.csm.search_cost(name, search_internal_id, internal_id_cost);
    }

    // -- RELATIONSHIP --
    void create_relationship(const std::string &name)
    {
        auto &data = generator.action_data();
        data.actions[name] = ClauseAction::CreateRelationship;
        auto nodes = visited_nodes.last_two();
        data.relationship_data.emplace(name,
                                       RelationshipData(nodes, direction));
    }

    void visit(ast::Relationship &ast_relationship) override
    {
        auto &cypher_data = generator.cypher_data();

        if (ast_relationship.has_name()) entity = ast_relationship.name();

        // TODO: simplify somehow
        if (state == CypherState::Create) {
            if (ast_relationship.has_name()) {
                auto name = ast_relationship.name();
                if (!cypher_data.exist(name)) {
                    clause_action = ClauseAction::CreateRelationship;
                    create_relationship(name);
                }
            }
        }

        using ast_direction = ast::Relationship::Direction;
        using generator_direction = RelationshipData::Direction;

        if (ast_relationship.direction == ast_direction::Left)
            direction = generator_direction::Left;

        if (ast_relationship.direction == ast_direction::Right)
            direction = generator_direction::Right;

        Traverser::visit(ast_relationship);

        // TODO: add suport for Direction::Both
    }

    void visit(ast::RelationshipSpecs &ast_relationship_specs) override
    {
        Traverser::visit(ast_relationship_specs);
    }

    void visit(ast::RelationshipTypeList &ast_relationship_type_list) override
    {
        auto &data = generator.action_data();

        if (ast_relationship_type_list.has_value()) {
            auto type = ast_relationship_type_list.value->name;
            data.add_entity_tag(entity, type);
        }

        Traverser::visit(ast_relationship_type_list);
    }

    void visit(ast::LabelList &ast_label_list) override
    {
        auto &data = generator.action_data();

        if (!ast_label_list.has_value()) return;

        auto label = ast_label_list.value->name;

        data.add_entity_tag(entity, label);

        Traverser::visit(ast_label_list);
    }

    void visit(ast::PropertyList& ast_property_list) override
    {
        Traverser::visit(ast_property_list);
    }

    void visit(ast::Property &ast_property) override
    {
        auto &data = generator.action_data();

        if (!ast_property.has_name()) return;

        auto name = ast_property.name();

        data.parameter_index.emplace(ParameterIndexKey(entity, name), index++);

        Traverser::visit(ast_property);
    }

    // -- SET --

    void visit(ast::SetList &ast_set_list) override
    {
        Traverser::visit(ast_set_list);
    }

    void visit(ast::SetElement &ast_set_element) override
    {
        Traverser::visit(ast_set_element);
    }

    void visit(ast::Accessor &ast_accessor) override
    {
        auto &data = generator.action_data();

        if (ast_accessor.has_entity() && ast_accessor.has_prop()) {
            auto entity = ast_accessor.entity_name();
            auto prop = ast_accessor.entity_prop();
            data.parameter_index.emplace(ParameterIndexKey(entity, prop),
                                         index++);
        }
    }
};
