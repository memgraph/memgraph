namespace query {

namespace test_common {

///
/// Create PropertyLookup with given name and property.
///
/// Name is used to create the Identifier which is used for property lookup.
///
auto GetPropertyLookup(AstTreeStorage &storage, const std::string &name,
                       GraphDb::Property property) {
  return storage.Create<PropertyLookup>(storage.Create<Identifier>(name),
                                        property);
}

///
/// Create an EdgeAtom with given name, edge_type and direction.
///
/// Name is used to create the Identifier which is assigned to the edge.
///
auto GetEdge(AstTreeStorage &storage, const std::string &name,
             GraphDb::EdgeType edge_type = nullptr,
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
             GraphDb::Label label = nullptr) {
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
auto GetQuery(AstTreeStorage &storage, std::vector<Clause *> clauses) {
  auto query = storage.query();
  query->clauses_.insert(query->clauses_.begin(), clauses.begin(),
                         clauses.end());
  return query;
}

///
/// Create the return clause with given named expressions.
///
auto GetReturn(AstTreeStorage &storage,
               std::vector<NamedExpression *> named_exprs) {
  auto ret = storage.Create<Return>();
  ret->named_expressions_.insert(ret->named_expressions_.begin(),
                                 named_exprs.begin(), named_exprs.end());
  return ret;
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
#define RETURN(...) query::test_common::GetReturn(storage, {__VA_ARGS__})
#define DELETE(...) query::test_common::GetDelete(storage, {__VA_ARGS__})
#define DETACH_DELETE(...) \
  query::test_common::GetDelete(storage, {__VA_ARGS__}, true)
#define QUERY(...) query::test_common::GetQuery(storage, {__VA_ARGS__})
#define LESS(expr1, expr2) storage.Create<query::LessOperator>((expr1), (expr2))
