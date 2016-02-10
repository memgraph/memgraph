#pragma once

#include "cypher/visitor/traverser.hpp"
#include "storage/model/properties/properties.hpp"

struct NodeTraverser : public Traverser
{
    Properties traverse(ast::Node& node)
    {
        Traverser::visit(node);
        return properties;
    }

    void visit(ast::Property& property) override
    {
        key = property.idn->name;
        Traverser::visit(property);
        if (flag == Property::Flags::Int32) {
            properties.set<Int32>(key, *int_value);
        }
        if (flag == Property::Flags::String) {
            properties.set<String>(key, *string_value);
        }
    }

    void visit(ast::String& string) override
    {
        flag = Property::Flags::String;
        string_value = std::make_shared<String>(string.value);
    }

    void visit(ast::Integer& integer) override
    {
        flag = Property::Flags::Int32;
        int_value = std::make_shared<Int32>(integer.value);
    }

    void visit(ast::Node& node) override
    {
        Traverser::visit(node);
    }

private:
    std::string key;
    Property::Flags flag;
    std::shared_ptr<Int32> int_value;
    std::shared_ptr<String> string_value;
    Properties properties;
};
