#ifndef MEMGRAPH_CYPHER_VISITOR_TRAVERSER_HPP
#define MEMGRAPH_CYPHER_VISITOR_TRAVERSER_HPP

#include "cypher/ast/ast_visitor.hpp"
#include "cypher/ast/ast.hpp"

class Traverser : public ast::AstVisitor
{
public:

    using uptr = std::unique_ptr<Traverser>;
    using sptr = std::shared_ptr<Traverser>;

    void visit(ast::Start& start) override
    {
        accept(start.read_query);
        accept(start.write_query);
    }
    
    void visit(ast::ReadQuery& read_query) override
    {
        accept(read_query.match);
        accept(read_query.return_clause);
    }

    void visit(ast::Match& match) override
    {
        accept(match.pattern);
        accept(match.where);
    }

    void visit(ast::Pattern& pattern) override
    {
        accept(pattern.node);
        accept(pattern.relationship);
        accept(pattern.next);
    }

    void visit(ast::Node& node) override
    {
        accept(node.idn);
        accept(node.labels);
        accept(node.props);
    }

    void visit(ast::Return& return_clause) override
    {
        accept(return_clause.return_list);
    }

    void visit(ast::Accessor& accessor) override
    {
        accept(accessor.entity);
        accept(accessor.prop);
    }
    void visit(ast::Property& property) override
    {
        accept(property.idn);
        accept(property.value);
    }

    void visit(ast::And& and_expr) override
    {
        accept(and_expr.left);
        accept(and_expr.right);
    }

    void visit(ast::Or& or_expr) override
    {
        accept(or_expr.left);
        accept(or_expr.right);
    }

    void visit(ast::Lt& lt_expr) override
    {
        accept(lt_expr.left);
        accept(lt_expr.right);
    }

    void visit(ast::Gt& gt_expr) override
    {
        accept(gt_expr.left);
        accept(gt_expr.right);
    }

    void visit(ast::Ge& ge_expr) override
    {
        accept(ge_expr.left);
        accept(ge_expr.right);
    }

    void visit(ast::Le& le_expr) override
    {
        accept(le_expr.left);
        accept(le_expr.right);
    }

    void visit(ast::Eq& eq_expr) override
    {
        accept(eq_expr.left);
        accept(eq_expr.right);
    }

    void visit(ast::Ne& ne_expr) override
    {
        accept(ne_expr.left);
        accept(ne_expr.right);
    }

    void visit(ast::Plus& plus) override
    {
        accept(plus.left);
        accept(plus.right);
    }

    void visit(ast::Minus& minus) override
    {
        accept(minus.left);
        accept(minus.right);
    }

    void visit(ast::Star& star) override
    {
        accept(star.left);
        accept(star.right);
    }

    void visit(ast::Slash& slash) override
    {
        accept(slash.left);
        accept(slash.right);
    }

    void visit(ast::Rem& rem) override
    {
        accept(rem.left);
        accept(rem.right);
    }

    void visit(ast::PropertyList& prop_list) override
    {
        accept(prop_list.value);
        accept(prop_list.next);
    }

    void visit(ast::RelationshipList& rel_list) override
    {
        accept(rel_list.value);
        accept(rel_list.next);
    }

    void visit(ast::Relationship& rel) override
    {
        accept(rel.specs);
    }

    void visit(ast::RelationshipSpecs& rel_specs) override
    {
        accept(rel_specs.idn);
        accept(rel_specs.types);
        accept(rel_specs.props);
    }

    void visit(ast::LabelList& labels) override
    {
        accept(labels.value);
        accept(labels.next);
    }

    void visit(ast::ReturnList& return_list) override
    {
        accept(return_list.value);
        accept(return_list.next);
    }

    void visit(ast::Where& where) override
    {
        accept(where.expr);
    }

    void visit(ast::WriteQuery& write_query) override
    {
        accept(write_query.create);
        accept(write_query.return_clause);
    }

    void visit(ast::Create& create) override
    {
        accept(create.pattern);
    }

protected:
    template<class T>
    void accept(T* node)
    {
        if(node != nullptr)
            node->accept(*this);
    }
};

#endif
