#pragma once

#include <cstdint>
#include <map>
#include <string>

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
    NotFound,
    Matched,
    Created
};

enum class EntityType : uint8_t
{
    NotFound,
    Node,
    Relationship
};

class CypherStateData
{
private:
    std::map<std::string, EntityStatus> entity_status;
    std::map<std::string, EntityType> entity_type;
    // TODO: container that keeps track about c++ variable names

public:
    bool exist(const std::string& name) const
    {
        return entity_status.find(name) != entity_status.end();
    }

    EntityStatus status(const std::string &name)
    {
        if (entity_status.find(name) == entity_status.end())
            return EntityStatus::NotFound;

        return entity_status.at(name);
    }

    EntityType type(const std::string &name)
    {
        if (entity_type.find(name) == entity_type.end())
            return EntityType::NotFound;

        return entity_type.at(name);
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
};
