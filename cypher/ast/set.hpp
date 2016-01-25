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
};

struct SetElement : public AstNode<SetElement>
{
    SetElement(Accessor* accessor, SetValue* set_value)
        : accessor(accessor), set_value(set_value) {}

    Accessor* accessor;
    SetValue* set_value;
};

struct SetList : public List<SetElement, SetList>
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
