#pragma once

#include <string>

#include "logging/default.hpp"
#include "query/backend/cpp_old/code.hpp"
#include "query/backend/cpp_old/cpp_generator.hpp"
#include "query/backend/cpp_old/entity_search.hpp"
#include "query/backend/cpp_old/structures.hpp"
#include "query/language/cypher/errors.hpp"
#include "query/language/cypher/visitor/traverser.hpp"

struct SetElementState
{
    std::string set_entity;
    std::string set_prop;
    int64_t set_index;

    SetElementState() { clear(); }

    bool is_defined() const
    {
        if (set_entity.empty()) return false;
        if (set_prop.empty()) return false;
        if (set_index < 0) return false;

        return true;
    }

    void clear()
    {
        set_entity = "";
        set_prop   = "";
        set_index  = -1;
    }
};

struct PropertyState
{
    std::string property_name;
    int64_t property_index;

    PropertyState() { clear(); }

    bool is_defined() const
    {
        if (property_name.empty()) return false;
        if (property_index < 0) return false;

        return true;
    }

    void clear()
    {
        property_name  = "";
        property_index = -1;
    }
};

// TODO: another idea is to user ast history in order to choose appropriate
// action

class CppTraverser : public Traverser, public Code
{
private:
    // currently active entity name (node or relationship name)
    std::string entity;

    // currently active state
    CypherState state; // TODO: move to generator
    QueryAction query_action;
    ClauseAction clause_action;

    // linearization
    CppGenerator generator;

    Vector<std::string> visited_nodes;

    RelationshipData::Direction direction;

    SetElementState set_element_state;
    LabelSetElement labels_set_element;
    PropertyState property_state;

    void clear_state()
    {
        set_element_state.clear();
        property_state.clear();
    }

    // for the first release every query has to have a RETURN clause
    // problem is where to put t.commit()
    // TODO: remove this constraint
    bool has_return;

    void finish_query_execution()
    {
        generator.add_action(QueryAction::TransactionCommit);

        code += generator.generate();

        generator.clear();
    }

    Logger logger;

public:
    CppTraverser() : logger(logging::log->logger("CppTraverser")) {}

    void semantic_check() const
    {
        if (!has_return)
            throw CypherSemanticError(
                "The query doesn't have RETURN clause. Next "
                "releases will support query without RETURN "
                "clause");
    }

    void visit(ast::WriteQuery &write_query) override
    {
        // TODO: crate top level node (TransactionBegin can be
        // only at the one place)
        generator.add_action(QueryAction::TransactionBegin);

        Traverser::visit(write_query);

        // TODO: put this inside the top level mentioned above
        finish_query_execution();
    }

    void visit(ast::ReadQuery &read_query) override
    {
        generator.add_action(QueryAction::TransactionBegin);

        Traverser::visit(read_query);

        finish_query_execution();
    }

    void visit(ast::UpdateQuery &update_query) override
    {
        generator.add_action(QueryAction::TransactionBegin);

        Traverser::visit(update_query);

        finish_query_execution();
    }

    void visit(ast::DeleteQuery &delete_query) override
    {
        generator.add_action(QueryAction::TransactionBegin);

        Traverser::visit(delete_query);

        finish_query_execution();
    }

    void visit(ast::ReadWriteQuery &read_write_query) override
    {
        generator.add_action(QueryAction::TransactionBegin);

        Traverser::visit(read_write_query);

        finish_query_execution();
    }

    void visit(ast::Match &ast_match) override
    {
        state = CypherState::Match;

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
        code += generator.generate();
        generator.add_action(QueryAction::Create);

        state        = CypherState::Create;
        query_action = QueryAction::Create;

        Traverser::visit(ast_create);
    }

    void visit(ast::Set &ast_set) override
    {
        code += generator.generate();

        generator.add_action(QueryAction::Set);

        state        = CypherState::Set;
        query_action = QueryAction::Set;

        Traverser::visit(ast_set);

        code += generator.generate();
    }

    void visit(ast::Return &ast_return) override
    {
        has_return = true;

        generator.add_action(QueryAction::Return);

        state        = CypherState::Return;
        query_action = QueryAction::Return;

        Traverser::visit(ast_return);
    }

    void visit(ast::ReturnList &ast_return_list) override
    {
        Traverser::visit(ast_return_list);
    }

    void visit(ast::PatternList &ast_pattern_list) override
    {
        Traverser::visit(ast_pattern_list);
    }

    void visit(ast::Pattern &ast_pattern) override
    {
        // TODO: Is that traversal order OK for all cases? Probably NOT.
        if (ast_pattern.has_next())
        {
            visit(*ast_pattern.next);
            visit(*ast_pattern.node);
            visit(*ast_pattern.relationship);
        }
        else
        {
            Traverser::visit(ast_pattern);
        }
    }

    void visit(ast::Node &ast_node) override
    {
        auto &action_data = generator.action_data();
        auto &cypher_data = generator.cypher_data();

        if (!ast_node.has_identifier()) return;

        auto name = ast_node.idn->name;
        entity    = name;
        visited_nodes.push_back(name);

        if (state == CypherState::Match)
        {
            action_data.actions[name] = ClauseAction::MatchNode;
        }

        if (state == CypherState::Create)
        {
            if (cypher_data.status(name) == EntityStatus::Matched)
            {
                action_data.actions[name] = ClauseAction::MatchNode;
            }
            else
            {
                action_data.actions[name] = ClauseAction::CreateNode;
            }
        }

        Traverser::visit(ast_node);

        if (cypher_data.status(name) != EntityStatus::Matched &&
            state == CypherState::Create)
        {
            cypher_data.node_created(name);
        }
    }

    void visit(ast::And &ast_and) override { Traverser::visit(ast_and); }

    void visit(ast::InternalIdExpr &internal_id_expr) override
    {
        if (!internal_id_expr.has_entity()) return;
        if (!internal_id_expr.has_id()) return;

        auto name = internal_id_expr.entity_name();
        // because entity_id value will be index inside the parameters array
        auto index = internal_id_expr.entity_id();

        auto &data = generator.action_data();

        data.parameter_index[ParameterIndexKey(InternalId, name)] = index;
        data.csm.search_cost(name, entity_search::search_internal_id,
                             entity_search::internal_id_cost);
    }

    // -- RELATIONSHIP --
    void create_relationship(const std::string &name)
    {
        auto &data         = generator.action_data();
        data.actions[name] = ClauseAction::CreateRelationship;
        auto nodes         = visited_nodes.last_two();
        data.relationship_data.emplace(name,
                                       RelationshipData(nodes, direction));
    }

    void visit(ast::Relationship &ast_relationship) override
    {
        auto &cypher_data = generator.cypher_data();
        auto &action_data = generator.action_data();

        if (!ast_relationship.has_name()) return;
        entity = ast_relationship.name();

        using ast_direction       = ast::Relationship::Direction;
        using generator_direction = RelationshipData::Direction;

        if (ast_relationship.direction == ast_direction::Left)
            direction = generator_direction::Left;

        if (ast_relationship.direction == ast_direction::Right)
            direction = generator_direction::Right;

        // TODO: add suport for Direction::Both

        // TODO: simplify somehow
        if (state == CypherState::Create)
        {
            if (!cypher_data.exist(entity))
            {
                clause_action = ClauseAction::CreateRelationship;
                create_relationship(entity);
            }
        }

        if (state == CypherState::Match)
        {
            if (!cypher_data.exist(entity))
            {
                action_data.actions[entity] = ClauseAction::MatchRelationship;
            }
        }

        Traverser::visit(ast_relationship);
    }

    void visit(ast::RelationshipSpecs &ast_relationship_specs) override
    {
        if (state == CypherState::Match)
        {
            if (ast_relationship_specs.has_identifier())
            {
                auto name         = ast_relationship_specs.name();
                auto &cypher_data = generator.cypher_data();
                if (!cypher_data.exist(name))
                {
                    clause_action      = ClauseAction::MatchRelationship;
                    auto &data         = generator.action_data();
                    data.actions[name] = ClauseAction::MatchRelationship;
                }
            }
        }

        if (state == CypherState::Create)
        {
            if (ast_relationship_specs.has_identifier())
            {
                entity = ast_relationship_specs.name();
            }
        }

        Traverser::visit(ast_relationship_specs);
    }

    void visit(ast::RelationshipTypeList &ast_relationship_type_list) override
    {
        auto &action_data = generator.action_data();

        if (ast_relationship_type_list.has_value())
        {
            auto type = ast_relationship_type_list.value->name;
            action_data.add_entity_tag(entity, type);
            action_data.csm.search_cost(entity,
                                        entity_search::search_type_index,
                                        entity_search::type_cost);
        }

        Traverser::visit(ast_relationship_type_list);
    }

    void visit(ast::LabelList &ast_label_list) override
    {
        if (state == CypherState::Set)
        {
            clause_action = ClauseAction::UpdateEntityLabels_Labels;
            Traverser::visit(ast_label_list);
            return;
        }

        auto &action_data = generator.action_data();
        auto &cypher_data = generator.cypher_data();

        if (!ast_label_list.has_value()) return;

        auto label = ast_label_list.value->name;

        action_data.add_entity_tag(entity, label);
        action_data.csm.search_cost(entity, entity_search::search_label_index,
                                    entity_search::label_cost);
        cypher_data.tag(entity, label);
        // TODO: it shouldn't be decided here
        cypher_data.source(entity, EntitySource::LabelIndex);

        Traverser::visit(ast_label_list);
    }

    void visit(ast::PropertyList &ast_property_list) override
    {
        Traverser::visit(ast_property_list);
    }

    void visit(ast::Property &ast_property) override
    {
        clear_state();

        Traverser::visit(ast_property);

        // TODO: too ugly refactor somehow (clear_state part is awful)
        if (entity.empty() || !property_state.is_defined())
        {
            clear_state();
            return;
        }

        auto prop  = property_state.property_name;
        auto index = property_state.property_index;

        // update action data
        auto &action_data = generator.action_data();
        action_data.parameter_index.emplace(ParameterIndexKey(entity, prop),
                                            index);
        action_data.add_entitiy_property(entity, prop);

        // update cypher data
        auto &cypher_data = generator.cypher_data();
        cypher_data.index(entity, prop, index);

        clear_state();
    }

    void visit(ast::Identifier &ast_identifier) override
    {
        property_state.property_name = ast_identifier.name;

        if (state == CypherState::Delete)
        {
            auto &action_data = generator.action_data();
            auto name         = ast_identifier.name;
            auto &cypher_data = generator.cypher_data();
            if (cypher_data.type(name) == EntityType::Node)
                action_data.actions[name] = ClauseAction::DeleteNode;
            if (cypher_data.type(name) == EntityType::Relationship)
                action_data.actions[name] = ClauseAction::DeleteRelationship;
        }

        if (state == CypherState::Set &&
            clause_action == ClauseAction::UpdateEntityLabels_Identifier)
        {
            labels_set_element.entity = ast_identifier.name;
        }

        if (state == CypherState::Set &&
            clause_action == ClauseAction::UpdateEntityLabels_Labels)
        {
            labels_set_element.labels.emplace_back(ast_identifier.name);
        }
    }

    void visit(ast::Long &ast_long) override
    {
        set_element_state.set_index   = ast_long.value;
        property_state.property_index = ast_long.value;
    }

    // -- SET subtree (node)
    //      QUERY: SET n.name = "bla", ...
    //      STRIP: SET n.name = 0, ...
    //      STATE: entity: n; prop: name; set_index: 0

    void visit(ast::SetList &ast_set_list) override
    {
        Traverser::visit(ast_set_list);
    }

    void visit(ast::SetElement &ast_set_element) override
    {
        Traverser::visit(ast_set_element);

        if (!set_element_state.is_defined())
        {
            clear_state();
            return;
        }

        auto entity = set_element_state.set_entity;
        auto prop   = set_element_state.set_prop;
        auto index  = set_element_state.set_index;

        auto &cypher_data = generator.cypher_data();
        auto entity_type  = cypher_data.type(entity);

        if (entity_type == EntityType::None)
            throw CypherSemanticError("Entity (" + entity + ") doesn't exist");

        auto &action_data = generator.action_data();

        if (entity_type == EntityType::Node)
            action_data.actions[entity] = ClauseAction::UpdateNode;
        if (entity_type == EntityType::Relationship)
            action_data.actions[entity] = ClauseAction::UpdateRelationship;

        action_data.parameter_index.emplace(ParameterIndexKey(entity, prop),
                                            index);
        action_data.add_entitiy_property(entity, prop);

        clear_state();
    }

    void visit(ast::Accessor &ast_accessor) override
    {
        if (!ast_accessor.has_entity()) return;

        auto &action_data = generator.action_data();

        if (state == CypherState::Return)
        {
            auto &return_elements = action_data.return_elements;
            auto &entity          = ast_accessor.entity_name();
            if (!ast_accessor.has_prop())
            {
                return_elements.emplace_back(ReturnElement(entity));
            }
            else
            {
                auto &property = ast_accessor.entity_prop();
                return_elements.emplace_back(ReturnElement(entity, property));
            }
        }

        if (!ast_accessor.has_prop()) return;

        if (state == CypherState::Set)
        {
            set_element_state.set_entity = ast_accessor.entity_name();
            set_element_state.set_prop   = ast_accessor.entity_prop();
        }
    }

    void visit(ast::SetValue &ast_set_value) override
    {
        Traverser::visit(ast_set_value);
    }

    void visit(ast::LabelSetElement &ast_label_set_element) override
    {
        clause_action = ClauseAction::UpdateEntityLabels_Identifier;

        labels_set_element.clear();

        Traverser::visit(ast_label_set_element);

        auto &action_data = generator.action_data();

        action_data.label_set_elements.emplace_back(std::move(labels_set_element));

        clause_action = ClauseAction::Undefined;
    }

    void visit(ast::Delete &ast_delete) override
    {
        code += generator.generate();

        state = CypherState::Delete;

        auto &action_data = generator.add_action(QueryAction::Delete);

        Traverser::visit(ast_delete);

        code += generator.generate();
    }

    void visit(ast::CountFunction &ast_count) override
    {
        auto &action_data = generator.action_data();
        auto &cypher_data = generator.cypher_data();

        // if (state == CypherState::Return)
        // {
        action_data.actions[ast_count.argument] = ClauseAction::ReturnCount;
        // }
    }

    void visit(ast::LabelsFunction &ast_label) override
    {
        auto &action_data = generator.action_data();
        action_data.actions[ast_label.argument] = ClauseAction::ReturnLabels;
    }
};
