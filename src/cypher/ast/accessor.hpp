#pragma once

#include <memory>
#include <string>

#include "expr.hpp"
#include "identifier.hpp"

namespace ast
{

struct Accessor : public ValueExpr<Accessor>
{
    Accessor(Identifier* entity, Identifier* prop)
        : entity(entity), prop(prop) {}

    Identifier* entity;
    Identifier* prop;

    bool has_entity() const { return entity != nullptr; }
    bool has_prop() const { return prop != nullptr; }

    std::string entity_name() const { return entity->name; }
    std::string entity_prop() const { return prop->name; }
};

}
