///
/// @file
/// This file provides macros for easier construction of openCypher query AST.
/// The usage of macros is very similar to how one would write openCypher. For
/// example:
///
///     AstTreeStorage storage;  // Macros rely on storage being in scope.
///
///     QUERY(MATCH(PATTERN(NODE("n"), EDGE("e"), NODE("m"))),
///           WHERE(LESS(PROPERTY_LOOKUP("e", edge_prop), LITERAL(3))),
///           RETURN(SUM(PROPERTY_LOOKUP("m", prop)), AS("sum"),
///                  ORDER_BY(IDENT("sum")),
///                  SKIP(ADD(LITERAL(1), LITERAL(2)))));
///
/// Each of the macros is accompanied by a function. The functions use overload
/// resolution and template magic to provide a type safe way of constructing
/// queries. Although the functions can be used by themselves, it is more
/// convenient to use the macros.
///

#include <utility>
#include <vector>

#include "database/graph_db_datatypes.hpp"
#include "query/frontend/ast/ast.hpp"

namespace query {

namespace test_common {

// Custom types for ORDER BY, SKIP and LIMIT and expressions, so that they can
// be used to resolve function calls.
struct OrderBy {
  std::vector<std::pair<Ordering, Expression *>> expressions;
};
struct Skip {
  Expression *expression = nullptr;
};
struct Limit {
  Expression *expression = nullptr;
};

// Helper functions for filling the OrderBy with expressions.
auto FillOrderBy(OrderBy &order_by, Expression *expression,
                 Ordering ordering = Ordering::ASC) {
  order_by.expressions.emplace_back(ordering, expression);
}
template <class... T>
auto FillOrderBy(OrderBy &order_by, Expression *expression, Ordering ordering,
                 T... rest) {
  FillOrderBy(order_by, expression, ordering);
  FillOrderBy(order_by, rest...);
}
template <class... T>
auto FillOrderBy(OrderBy &order_by, Expression *expression, T... rest) {
  FillOrderBy(order_by, expression);
  FillOrderBy(order_by, rest...);
}

///
/// Create OrderBy expressions.
///
/// The supported combination of arguments is: (Expression, [Ordering])+
/// Since the Ordering is optional, by default it is ascending.
///
template <class... T>
auto GetOrderBy(T... exprs) {
  OrderBy order_by;
  FillOrderBy(order_by, exprs...);
  return order_by;
}

///
/// Create PropertyLookup with given name and property.
///
/// Name is used to create the Identifier which is used for property lookup.
///
auto GetPropertyLookup(AstTreeStorage &storage, const std::string &name,
                       GraphDbTypes::Property property) {
  return storage.Create<PropertyLookup>(storage.Create<Identifier>(name),
                                        property);
}

///
/// Create an EdgeAtom with given name, edge_type and direction.
///
/// Name is used to create the Identifier which is assigned to the edge.
///
auto GetEdge(AstTreeStorage &storage, const std::string &name,
             GraphDbTypes::EdgeType edge_type = nullptr,
             EdgeAtom::Direction dir = EdgeAtom::Direction::BOTH) {
  auto edge = storage.Create<EdgeAtom>(storage.Create<Identifier>(name), dir);
  if (edge_type) edge->edge_types_.emplace_back(edge_type);
  return edge;
}

///
/// Create an EdgeAtom with given name and direction.
///
auto GetEdge(AstTreeStorage &storage, const std::string &name,
             EdgeAtom::Direction dir) {
  return GetEdge(storage, name, nullptr, dir);
}

///
/// Create a NodeAtom with given name and label.
///
/// Name is used to create the Identifier which is assigned to the node.
///
auto GetNode(AstTreeStorage &storage, const std::string &name,
             GraphDbTypes::Label label = nullptr) {
  auto node = storage.Create<NodeAtom>(storage.Create<Identifier>(name));
  if (label) node->labels_.emplace_back(label);
  return node;
}

///
/// Create a Pattern with given atoms.
///
auto GetPattern(AstTreeStorage &storage, std::vector<PatternAtom *> atoms) {
  auto pattern = storage.Create<Pattern>();
  pattern->atoms_.insert(pattern->atoms_.begin(), atoms.begin(), atoms.end());
  return pattern;
}

///
/// This function creates an AST node which can store patterns and fills them
/// with given patterns.
///
/// The function is most commonly used to create Match and Create clauses.
///
template <class TWithPatterns>
auto GetWithPatterns(AstTreeStorage &storage, std::vector<Pattern *> patterns) {
  auto with_patterns = storage.Create<TWithPatterns>();
  with_patterns->patterns_.insert(with_patterns->patterns_.begin(),
                                  patterns.begin(), patterns.end());
  return with_patterns;
}

///
/// Create a query with given clauses.
///
auto GetQuery(AstTreeStorage &storage, Clause *clause) {
  storage.query()->clauses_.emplace_back(clause);
  return storage.query();
}
auto GetQuery(AstTreeStorage &storage, Match *match, Where *where) {
  match->where_ = where;
  storage.query()->clauses_.emplace_back(match);
  return storage.query();
}
auto GetQuery(AstTreeStorage &storage, With *with, Where *where) {
  with->where_ = where;
  storage.query()->clauses_.emplace_back(with);
  return storage.query();
}
template <class... T>
auto GetQuery(AstTreeStorage &storage, Match *match, Where *where,
              T *... clauses) {
  match->where_ = where;
  storage.query()->clauses_.emplace_back(match);
  return GetQuery(storage, clauses...);
}
template <class... T>
auto GetQuery(AstTreeStorage &storage, With *with, Where *where,
              T *... clauses) {
  with->where_ = where;
  storage.query()->clauses_.emplace_back(with);
  return GetQuery(storage, clauses...);
}
template <class... T>
auto GetQuery(AstTreeStorage &storage, Clause *clause, T *... clauses) {
  storage.query()->clauses_.emplace_back(clause);
  return GetQuery(storage, clauses...);
}

// Helper functions for constructing RETURN and WITH clauses.
void FillReturnBody(ReturnBody &body, NamedExpression *named_expr) {
  body.named_expressions.emplace_back(named_expr);
}
void FillReturnBody(ReturnBody &body, Limit limit) {
  body.limit = limit.expression;
}
void FillReturnBody(ReturnBody &body, Skip skip, Limit limit = Limit{}) {
  body.skip = skip.expression;
  body.limit = limit.expression;
}
void FillReturnBody(ReturnBody &body, OrderBy order_by, Limit limit = Limit{}) {
  body.order_by = order_by.expressions;
  body.limit = limit.expression;
}
void FillReturnBody(ReturnBody &body, OrderBy order_by, Skip skip,
                    Limit limit = Limit{}) {
  body.order_by = order_by.expressions;
  body.skip = skip.expression;
  body.limit = limit.expression;
}
void FillReturnBody(ReturnBody &body, Expression *expr,
                    NamedExpression *named_expr) {
  // This overload supports `RETURN(expr, AS(name))` construct, since
  // NamedExpression does not inherit Expression.
  named_expr->expression_ = expr;
  body.named_expressions.emplace_back(named_expr);
}
template <class... T>
void FillReturnBody(ReturnBody &body, Expression *expr,
                    NamedExpression *named_expr, T... rest) {
  named_expr->expression_ = expr;
  body.named_expressions.emplace_back(named_expr);
  FillReturnBody(body, rest...);
}
template <class... T>
void FillReturnBody(ReturnBody &body, NamedExpression *named_expr, T... rest) {
  body.named_expressions.emplace_back(named_expr);
  FillReturnBody(body, rest...);
}

///
/// Create the return clause with given expressions.
///
/// The supported expression combination of arguments is:
///
/// (NamedExpression | (Expression NamedExpression))+ [OrderBy] [Skip] [Limit]
///
/// When the pair (Expression NamedExpression) is given, the Expression will be
/// moved inside the NamedExpression. This is done, so that the constructs like
/// RETURN(expr, AS("name"), ...) are supported.
///
/// @sa GetWith
template <class... T>
auto GetReturn(AstTreeStorage &storage, T... exprs) {
  auto ret = storage.Create<Return>();
  FillReturnBody(ret->body_, exprs...);
  return ret;
}

///
/// Create the with clause with given expressions.
///
/// The supported expression combination is the same as for @c GetReturn.
///
/// @sa GetReturn
template <class... T>
auto GetWith(AstTreeStorage &storage, T... exprs) {
  auto with = storage.Create<With>();
  FillReturnBody(with->body_, exprs...);
  return with;
}

///
/// Create the delete clause with given named expressions.
///
auto GetDelete(AstTreeStorage &storage, std::vector<Expression *> exprs,
               bool detach = false) {
  auto del = storage.Create<Delete>();
  del->expressions_.insert(del->expressions_.begin(), exprs.begin(),
                           exprs.end());
  del->detach_ = detach;
  return del;
}

///
/// Create a set property clause for given property lookup and the right hand
/// side expression.
///
auto GetSet(AstTreeStorage &storage, PropertyLookup *prop_lookup,
            Expression *expr) {
  return storage.Create<SetProperty>(prop_lookup, expr);
}

///
/// Create a set properties clause for given identifier name and the right hand
/// side expression.
///
auto GetSet(AstTreeStorage &storage, const std::string &name, Expression *expr,
            bool update = false) {
  return storage.Create<SetProperties>(storage.Create<Identifier>(name), expr,
                                       update);
}

///
/// Create a set labels clause for given identifier name and labels.
///
auto GetSet(AstTreeStorage &storage, const std::string &name,
            std::vector<GraphDbTypes::Label> labels) {
  return storage.Create<SetLabels>(storage.Create<Identifier>(name), labels);
}

///
/// Create a remove property clause for given property lookup
///
auto GetRemove(AstTreeStorage &storage, PropertyLookup *prop_lookup) {
  return storage.Create<RemoveProperty>(prop_lookup);
}

///
/// Create a remove labels clause for given identifier name and labels.
///
auto GetRemove(AstTreeStorage &storage, const std::string &name,
               std::vector<GraphDbTypes::Label> labels) {
  return storage.Create<RemoveLabels>(storage.Create<Identifier>(name), labels);
}

}  // namespace test_common

}  // namespace query

///
/// All the following macros implicitly pass `storage` variable to functions.
/// You need to have `AstTreeStorage storage;` somewhere in scope to use them.
/// Refer to function documentation to see what the macro does.
///
/// Example usage:
///
///   // Create MATCH (n) -[r]- (m) RETURN m AS new_name
///   AstTreeStorage storage;
///   auto query = QUERY(MATCH(PATTERN(NODE("n"), EDGE("r"), NODE("m"))),
///                      RETURN(NEXPR("new_name"), IDENT("m")));
///
#define NODE(...) query::test_common::GetNode(storage, __VA_ARGS__)
#define EDGE(...) query::test_common::GetEdge(storage, __VA_ARGS__)
#define PATTERN(...) query::test_common::GetPattern(storage, {__VA_ARGS__})
#define MATCH(...) \
  query::test_common::GetWithPatterns<query::Match>(storage, {__VA_ARGS__})
#define WHERE(expr) storage.Create<query::Where>((expr))
#define CREATE(...) \
  query::test_common::GetWithPatterns<query::Create>(storage, {__VA_ARGS__})
#define IDENT(name) storage.Create<query::Identifier>((name))
#define LITERAL(val) storage.Create<query::Literal>((val))
#define PROPERTY_LOOKUP(...) \
  query::test_common::GetPropertyLookup(storage, __VA_ARGS__)
#define NEXPR(name, expr) storage.Create<query::NamedExpression>((name), (expr))
// AS is alternative to NEXPR which does not initialize NamedExpression with
// Expression. It should be used with RETURN or WITH. For example:
// RETURN(IDENT("n"), AS("n")) vs. RETURN(NEXPR("n", IDENT("n"))).
#define AS(name) storage.Create<query::NamedExpression>((name))
#define RETURN(...) query::test_common::GetReturn(storage, __VA_ARGS__)
#define WITH(...) query::test_common::GetWith(storage, __VA_ARGS__)
#define ORDER_BY(...) query::test_common::GetOrderBy(__VA_ARGS__)
#define SKIP(expr) \
  query::test_common::Skip { (expr) }
#define LIMIT(expr) \
  query::test_common::Limit { (expr) }
#define DELETE(...) query::test_common::GetDelete(storage, {__VA_ARGS__})
#define DETACH_DELETE(...) \
  query::test_common::GetDelete(storage, {__VA_ARGS__}, true)
#define SET(...) query::test_common::GetSet(storage, __VA_ARGS__)
#define REMOVE(...) query::test_common::GetRemove(storage, __VA_ARGS__)
#define QUERY(...) query::test_common::GetQuery(storage, __VA_ARGS__)
// Various operators
#define ADD(expr1, expr2) \
  storage.Create<query::AdditionOperator>((expr1), (expr2))
#define LESS(expr1, expr2) storage.Create<query::LessOperator>((expr1), (expr2))
#define SUM(expr) \
  storage.Create<query::Aggregation>((expr), query::Aggregation::Op::SUM)
#define COUNT(expr) \
  storage.Create<query::Aggregation>((expr), query::Aggregation::Op::COUNT)
