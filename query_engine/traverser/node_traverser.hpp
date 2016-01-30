#pragma once

#include <iostream>
#include <typeinfo>
#include <map>

#include "cypher/visitor/traverser.hpp"

using std::cout;
using std::endl;

class NodeTraverser : public Traverser
{
    struct PropertiesTraverser : public Traverser
    {
        PropertiesTraverser(NodeTraverser* node_traverser)
            : node_traverser(node_traverser) {}

        // friend NodeTraverser;

        void visit(ast::Property& property) override
        {
            name = property.idn->name;
            Traverser::visit(property);
            json[name] = value;
        }

        void visit(ast::String& string) override
        {
            value = string.value;
        }

        void visit(ast::Integer& integer) override
        {
            value = std::to_string(integer.value);
        }

        void visit(ast::Node& node) override
        {
            Traverser::visit(node);
            node_traverser->json = json;
        }
    private:
        std::string name;
        std::string value;
        std::map<std::string, std::string> json;
        NodeTraverser* node_traverser;
    };

public:
    // TODO: replace with generic value
    std::map<std::string, std::string> json;

    void visit(ast::Create& create) override
    {
        auto create_nodes = PropertiesTraverser(this);
        create.accept(create_nodes);
    };
};
