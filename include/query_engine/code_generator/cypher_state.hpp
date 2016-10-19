#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "query_engine/code_generator/namer.hpp"
#include "query_engine/exceptions/exceptions.hpp"
#include "storage/model/properties/flags.hpp"

// main states that are used while ast is traversed
// in order to generate ActionSequence
enum class CypherState : uint8_t
{
    Undefined,
    Match,
    Where,
    Create,
    Set,
    Return,
    Delete
};

enum class EntityStatus : uint8_t
{
    None,
    Matched,
    Created
};

enum class EntityType : uint8_t
{
    None,
    Node,
    Relationship
};

// where OR how entity can be found
enum class EntitySource : uint8_t
{
    None,
    InternalId,
    LabelIndex,
    TypeIndex,
    MainStorage
};

// TODO: reduce copying
class CypherStateData
{
public:
    using tags_type = std::vector<std::string>;
    using properties_type = std::map<std::string, int64_t>;

private:
    std::map<std::string, EntityStatus> entity_status;
    std::map<std::string, EntityType> entity_type;
    std::map<std::string, EntitySource> entity_source;
    std::map<std::string, tags_type> entity_tags;
    std::map<std::string, properties_type> entity_properties;

public:
    bool exist(const std::string &name) const
    {
        return entity_status.find(name) != entity_status.end();
    }

    EntityStatus status(const std::string &name)
    {
        if (entity_status.find(name) == entity_status.end())
            return EntityStatus::None;

        return entity_status.at(name);
    }

    EntityType type(const std::string &name) const
    {
        if (entity_type.find(name) == entity_type.end())
            return EntityType::None;

        return entity_type.at(name);
    }

    EntitySource source(const std::string &name) const
    {
        if (entity_source.find(name) == entity_source.end())
            return EntitySource::None;
        return entity_source.at(name);
    }

    const std::map<std::string, EntityType> &all_typed_enteties()
    {
        return entity_type;
    }

    void node_matched(const std::string &name)
    {
        entity_type[name] = EntityType::Node;
        entity_status[name] = EntityStatus::Matched;
    }

    void node_created(const std::string &name)
    {
        entity_type[name] = EntityType::Node;
        entity_status[name] = EntityStatus::Created;
    }

    void relationship_matched(const std::string &name)
    {
        entity_type[name] = EntityType::Relationship;
        entity_status[name] = EntityStatus::Matched;
    }

    void relationship_created(const std::string &name)
    {
        entity_type[name] = EntityType::Relationship;
        entity_status[name] = EntityStatus::Created;
    }

    void source(const std::string &name, EntitySource source)
    {
        entity_source[name] = source;
    }

    // entity tags
    auto tags(const std::string &name) const
    {
        if (entity_tags.find(name) == entity_tags.end())
            throw CppGeneratorException("No tags for specified entity");
        return entity_tags.at(name);
    }

    void tags(const std::string &name, tags_type tags)
    {
        entity_tags[name] = tags;
    }

    void tag(const std::string &name, const std::string &new_tag)
    {
        if (entity_tags.find(name) == entity_tags.end()) {
            entity_tags[name] = std::vector<std::string>{};
        }
        entity_tags[name].emplace_back(new_tag);
    }

    // entity properties
    auto properties(const std::string &name) const
    {
        if (entity_properties.find(name) == entity_properties.end())
            throw CppGeneratorException("No properties for specified entity");
        return entity_properties.at(name);
    }

    void properties(const std::string &name, properties_type properties)
    {
        entity_properties[name] = properties;
    }

    void index(const std::string &entity, const std::string &property,
               int64_t index)
    {
        if (entity_properties.find(entity) == entity_properties.end()) {
            entity_properties[entity] = properties_type{};
        }
        entity_properties[entity][property] = index;
    }

    auto index(const std::string &entity, const std::string &property_name)
    {
        if (entity_properties.find(entity) == entity_properties.end())
            throw CppGeneratorException("No properties for specified entity");

        auto properties = entity_properties.at(entity);

        if (properties.find(property_name) == properties.end())
            throw CppGeneratorException(
                "No property for specified property name");

        return properties[property_name];
    }

    using iter_t = properties_type::iterator;
    auto print_indices(const std::string &name,
                       const std::string &variable_name = "indices")
    {
        // TODO: find out smarter way it's not hard
        std::string print =
            "std::map<std::string, int64_t> " + variable_name + " = ";
        print += "{";
        auto indices = entity_properties.at(name);
        size_t i = 0;
        iter_t it = indices.begin();
        for (; it != indices.end(); ++it, ++i)
        {
            print +=
                "{\"" + it->first + "\"," + std::to_string(it->second) + "}";
            if (i < indices.size() - 1)
                print += ",";
        }
        print += "};";
        return print;
    }
};
