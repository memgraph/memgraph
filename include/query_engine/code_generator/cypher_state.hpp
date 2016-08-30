#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "query_engine/code_generator/namer.hpp"
#include "storage/model/properties/flags.hpp"
#include "query_engine/exceptions/exceptions.hpp"

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
    MainStorage 
};

class CypherStateData
{
private:
    std::map<std::string, EntityStatus> entity_status;
    std::map<std::string, EntityType> entity_type;
    std::map<std::string, EntitySource> entity_source;
    std::map<std::string, std::vector<std::string>> entity_tags;

    // TODO: container that keeps track about c++ variable names

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

    auto tags(const std::string& name) const
    {
        if (entity_tags.find(name) == entity_tags.end())
            throw CppGeneratorException("No tags for specified entity");
        return entity_tags.at(name);
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

    void source(const std::string& name, EntitySource source)
    {
        entity_source[name] = source;
    }

    void tags(const std::string& name, std::vector<std::string> tags)
    {
        entity_tags[name] = tags;
    }
};
