#pragma once

#include "accessor.hpp"
#include "list.hpp"
#include "expr.hpp"

namespace ast
{
//
//                    SetList
//                       ^
//                       |
//       |------------------------------------|
//                              SetElement
//                                  ^
//                                  |
//                        |-------------------|
//                         Accessor    SetValue
//                            ^           ^
//                            |           |
//                        |-------|   |-------|
//   SET n.name = "Paul", n.surname = "Scholes"
//       ^                     ^
//       |                     |
//  element entity        element property
//

struct SetValue : public AstNode<SetValue>
{
    SetValue(Expr* value)
        : value(value) {}

    Expr* value;

    bool has_value() const { return value != nullptr; }
};

struct SetElementBase : public AstVisitable
{
};

template <class Derived>
struct SetElementDerivedBase : public Crtp<Derived>, public SetElementBase
{
    using uptr = std::unique_ptr<Derived>;

    virtual void accept(AstVisitor &visitor) { visitor.visit(this->derived()); }
};

struct SetElement : public SetElementDerivedBase<SetElement>
{
    SetElement(Accessor* accessor, SetValue* set_value)
        : accessor(accessor), set_value(set_value) {}

    Accessor* accessor;
    SetValue* set_value;

    bool has_accessor() const { return accessor != nullptr; }
    bool has_value() const { return set_value != nullptr; }
};

struct LabelSetElement : public SetElementDerivedBase<LabelSetElement>
{
    LabelSetElement(Identifier* identifier, LabelList* label_list)
        : identifier(identifier), label_list(label_list) {}

    Identifier* identifier;
    LabelList* label_list;

    bool has_identifier() const { return identifier != nullptr; }
    bool has_label_list() const { return label_list != nullptr; }
};

struct SetList : public List<SetElementBase, SetList>
{
    using List::List;
};

struct Set : public AstNode<Set>
{
    Set(SetList* set_list)
        : set_list(set_list) {}

    SetList* set_list;
};

}
