#ifndef MEMGRAPH_CYPHER_CODEGEN_CPPGEN_HPP
#define MEMGRAPH_CYPHER_CODEGEN_CPPGEN_HPP

#include <iostream>
#include <typeinfo>

#include "cypher/visitor/traverser.hpp"

using std::cout;
using std::endl;

class CppGen : public Traverser
{
    struct CreateGen : public Traverser
    {
        void visit(ast::Pattern& pattern) override
        {
            Traverser::visit(pattern);
        }
        
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

        void visit(ast::Node& node) override
        {
            cout << "Node: " << node.idn->name << endl;
            Traverser::visit(node);
            for (auto& kv : json) {
                cout << "Key: " << kv.first << ", Value: " << kv.second << endl;
            }
        }
    private:
        std::string name;
        std::string value;
        std::map<std::string, std::string> json;
    };

public:

    void visit(ast::Create& create) override
    {
        auto create_gen = CreateGen();
        create.accept(create_gen);
    };
};

#endif
