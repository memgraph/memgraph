#pragma once

#include <iostream>
#include <map>
#include <vector>

#include "query_engine/code_generator/clause_action.hpp"
#include "query_engine/code_generator/entity_search.hpp"
#include "query_engine/exceptions/exceptions.hpp"
#include "storage/model/properties/all.hpp"
#include "utils/assert.hpp"
#include "utils/underlying_cast.hpp"

// used for storing data related to an entity (node or relationship)
// data can be:
//     * tags: labels or type
//     * props: property name, property value
struct EntityData
{
    std::vector<std::string> tags;
    std::vector<std::string> properties;

    void add_tag(const std::string &tag) { tags.push_back(tag); }
    void add_property(const std::string &property)
    {
        properties.push_back(property);
    }
};

// used for storing indices of parameters (parameters are stripped before
// compiling process into the array), so somehow the compiler has to know
// how to find appropriate parameter during the compile process
//
// parameter index key can be related to:
//     * internal_id and entity_name, e.g.:
//     ID(n)=35445 -> ID(n)=2 -> index[PropertyIndexKey(Type::InternalId, n)] =
//     2
//     * entity_name and entity_property, e.g.:
//     n.name = "test" -> n.name = 3 -> index[PropertyIndexKey(entity_name,
//     entity_property)] = 3
struct ParameterIndexKey
{
    enum class Type : uint8_t
    {
        InternalId,
        Projection
    };

    ParameterIndexKey(Type type, const std::string &entity_name)
        : type(type), entity_name(entity_name)
    {
    }

    ParameterIndexKey(const std::string &entity_name,
                      const std::string &entity_property)
        : type(Type::Projection), entity_name(entity_name),
          entity_property(entity_property)
    {
    }

    const Type type;
    const std::string entity_name;
    const std::string entity_property;

    bool operator<(const ParameterIndexKey &rhs) const
    {
        runtime_assert(type == rhs.type,
                       "ParameterIndexKey types should be the same");

        if (type == Type::InternalId) return entity_name < rhs.entity_name;

        if (entity_name == rhs.entity_name)
            return entity_property < rhs.entity_property;

        return entity_name < rhs.entity_name;
    }
};

struct RelationshipData
{
    enum class Direction
    {
        Left,
        Right
    };

    using nodes_t = std::pair<std::string, std::string>;

    RelationshipData(nodes_t nodes, Direction direction)
        : nodes(nodes), direction(direction)
    {
    }

    std::pair<std::string, std::string> nodes;
    Direction direction;
};

struct ReturnElement
{
    ReturnElement(const std::string &entity) : entity(entity) {}
    ReturnElement(const std::string &entity, const std::string &property)
        : entity(entity), property(property){};

    std::string entity;
    std::string property;

    bool has_entity() const { return !entity.empty(); }
    bool has_property() const { return !property.empty(); }

    bool is_entity_only() const { return has_entity() && !has_property(); }
    bool is_projection() const { return has_entity() && has_property(); }
};

struct QueryActionData
{
    std::map<ParameterIndexKey, uint64_t> parameter_index;
    std::map<std::string, ClauseAction> actions;
    std::map<std::string, EntityData> entity_data;
    std::map<std::string, RelationshipData> relationship_data;
    std::vector<ReturnElement> return_elements;
    CypherStateMachine csm;

    QueryActionData() = default;
    QueryActionData(QueryActionData &&other) = default;

    void create_entity(const std::string &entity)
    {
        if (entity_data.find(entity) == entity_data.end())
            entity_data.emplace(entity, EntityData());
    }

    void add_entity_tag(const std::string &entity, const std::string &tag)
    {
        create_entity(entity);
        entity_data.at(entity).add_tag(tag);
    }

    void add_entitiy_property(const std::string &entity,
                              const std::string &property)
    {
        create_entity(entity);
        entity_data.at(entity).add_property(property);
    }

    // TODO: refactor name
    auto get_entity_property(const std::string &entity) const
    {
        if (entity_data.find(entity) == entity_data.end())
            throw CppGeneratorException("Entity " + entity + " doesn't exist");

        return entity_data.at(entity);
    }

    auto get_tags(const std::string& entity) const
    {
        if (entity_data.find(entity) == entity_data.end())
            throw CppGeneratorException("Entity " + entity + "doesn't exist");

        return entity_data.at(entity).tags;
    }

    void print() const
    {
        for (auto const &action : actions) {
            std::cout << action.first << " " << underlying_cast(action.second)
                      << std::endl;
        }
    }
};
