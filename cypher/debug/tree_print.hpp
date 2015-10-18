#ifndef MEMGRAPH_CYPHER_TREE_PRINT_HPP
#define MEMGRAPH_CYPHER_TREE_PRINT_HPP

#include <iostream>
#include <stack>

#include "cypher/ast/ast_visitor.hpp"
#include "cypher/ast/ast.hpp"

class PrintVisitor : public ast::AstVisitor
{
    class Printer
    {
    public:
        Printer(std::ostream& stream, const std::string& header)
            : stream(stream)
        {
            stream << header;
        }

        ~Printer()
        {
            stream << std::endl;
        }

        class Entry
        {
        public:
            Entry(Printer& printer) : printer(printer), valid(true)
            {
                printer.level++;
    
                for(size_t i = 1; i < printer.level; ++i)
                    printer.stream << "|  ";

                printer.stream << "+--";
            }

            Entry(const Entry&) = delete;
            
            Entry(Entry&& other) : printer(other.printer), valid(true)
            {
                other.valid = false;
            }
            
            ~Entry()
            {
                if(valid)
                    printer.level--;
            }

            template <class T>
            friend Entry& operator<<(Entry& entry, const T& item)
            {
                entry.printer.stream << item;
                return entry;
            }

        private:
            Printer& printer;
            bool valid;
        };

        Entry advance()
        {
            stream << std::endl;
            return std::move(Entry(*this));
        }

        Entry advance(const std::string& text)
        {
            stream << std::endl;
            auto entry = Entry(*this);
            entry << text;
            return std::move(entry);
        }

    private:
        std::ostream& stream;
        size_t level = 0;
    };

public:
    PrintVisitor(std::ostream& stream)
        : printer(stream, "Printing AST") {}

    void visit(ast::Start& start) override
    {
        auto entry = printer.advance("Start");
        accept(start.read_query);
        accept(start.write_query);
    }
    
    void visit(ast::ReadQuery& read_query) override
    {
        auto entry = printer.advance("Read Query");
        accept(read_query.match);
        accept(read_query.return_clause);
    }

    void visit(ast::Match& match) override
    {
        auto entry = printer.advance("Match");
        accept(match.pattern);
        accept(match.where);
    }

    void visit(ast::Pattern& pattern) override
    {
        auto entry = printer.advance("Pattern");
        accept(pattern.node);
        accept(pattern.relationship);
        accept(pattern.next);
    }

    void visit(ast::Node& node) override
    {
        auto entry = printer.advance("Node");
        accept(node.idn);
        accept(node.labels);
        accept(node.props);
    }

    void visit(ast::Identifier& idn) override
    {
        auto entry = printer.advance();
        entry << "Identifier '" << idn.name << "'";
    }

    void visit(ast::Return& return_clause) override
    {
        auto entry = printer.advance("Return");
        accept(return_clause.return_list);
    }

    void visit(ast::Accessor& accessor) override
    {
        auto entry = printer.advance("Accessor");
        accept(accessor.entity);
        accept(accessor.prop);
    }

    void visit(ast::Boolean& boolean) override
    {
        auto entry = printer.advance();
        entry << "Boolean " << boolean.value;
    }

    void visit(ast::Float& floating) override
    {
        auto entry = printer.advance();
        entry << "Float " << floating.value;
    }

    void visit(ast::Integer& integer) override
    {
        auto entry = printer.advance();
        entry << "Integer " << integer.value;
    }

    void visit(ast::String& string) override
    {
        auto entry = printer.advance();
        entry << "String " << string.value;
    }

    void visit(ast::Property& property) override
    {
        auto entry = printer.advance("Property");
        accept(property.idn);
        accept(property.value);
    }

    void visit(ast::And& and_expr) override
    {
        auto entry = printer.advance("And");
        accept(and_expr.left);
        accept(and_expr.right);
    }

    void visit(ast::Or& or_expr) override
    {
        auto entry = printer.advance("Or");
        accept(or_expr.left);
        accept(or_expr.right);
    }

    void visit(ast::Lt& lt_expr) override
    {
        auto entry = printer.advance("Less Than");
        accept(lt_expr.left);
        accept(lt_expr.right);
    }

    void visit(ast::Gt& gt_expr) override
    {
        auto entry = printer.advance("Greater Than");
        accept(gt_expr.left);
        accept(gt_expr.right);
    }

    void visit(ast::Ge& ge_expr) override
    {
        auto entry = printer.advance("Greater od Equal");
        accept(ge_expr.left);
        accept(ge_expr.right);
    }

    void visit(ast::Le& le_expr) override
    {
        auto entry = printer.advance("Less or Equal");
        accept(le_expr.left);
        accept(le_expr.right);
    }

    void visit(ast::Eq& eq_expr) override
    {
        auto entry = printer.advance("Equal");
        accept(eq_expr.left);
        accept(eq_expr.right);
    }

    void visit(ast::Ne& ne_expr) override
    {
        auto entry = printer.advance("Not Equal");
        accept(ne_expr.left);
        accept(ne_expr.right);
    }

    void visit(ast::Plus& plus) override
    {
        auto entry = printer.advance("Plus");
        accept(plus.left);
        accept(plus.right);
    }

    void visit(ast::Minus& minus) override
    {
        auto entry = printer.advance("Minus");
        accept(minus.left);
        accept(minus.right);
    }

    void visit(ast::Star& star) override
    {
        auto entry = printer.advance("Star");
        accept(star.left);
        accept(star.right);
    }

    void visit(ast::Slash& slash) override
    {
        auto entry = printer.advance("Slash");
        accept(slash.left);
        accept(slash.right);
    }

    void visit(ast::Rem& rem) override
    {
        auto entry = printer.advance("Rem (%)");
        accept(rem.left);
        accept(rem.right);
    }

    void visit(ast::PropertyList& prop_list) override
    {
        auto entry = printer.advance("Property List");
        accept(prop_list.value);
        accept(prop_list.next);
    }

    void visit(ast::RelationshipList& rel_list) override
    {
        auto entry = printer.advance("Relationship List");
        accept(rel_list.value);
        accept(rel_list.next);
    }

    void visit(ast::Relationship& rel) override
    {
        auto entry = printer.advance("Relationship");
        entry << " direction: " << rel.direction;
        accept(rel.specs);
    }

    void visit(ast::RelationshipSpecs& rel_specs) override
    {
        auto entry = printer.advance("Relationship Specs");
        accept(rel_specs.idn);
        accept(rel_specs.types);
        accept(rel_specs.props);
    }

    void visit(ast::LabelList& labels) override
    {
        auto entry = printer.advance("Label List");
        accept(labels.value);
        accept(labels.next);
    }

    void visit(ast::ReturnList& return_list) override
    {
        auto entry = printer.advance("Return List");
        accept(return_list.value);
        accept(return_list.next);
    }

    void visit(ast::Where& where) override
    {
        auto entry = printer.advance("Where");
        accept(where.expr);
    }

    void visit(ast::WriteQuery& write_query) override
    {
        auto entry = printer.advance("Write Query");
        accept(write_query.create);
        accept(write_query.return_clause);
    }

    void visit(ast::Create& create) override
    {
        auto entry = printer.advance("Create");
        accept(create.pattern);
    }

private:
    Printer printer;

    template<class T>
    void accept(T* node)
    {
        if(node != nullptr)
            node->accept(*this);
    }
};

#endif
