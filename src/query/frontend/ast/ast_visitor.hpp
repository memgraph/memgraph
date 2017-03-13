#pragma once

namespace query {

// Forward declares for TreeVisitorBase
class Query;
class NamedExpression;
class Identifier;
class Match;
class Return;
class Pattern;
class NodeAtom;
class EdgeAtom;

class TreeVisitorBase {
public:
  virtual ~TreeVisitorBase() {}
  // Start of the tree is a Query.
  virtual void PreVisit(Query &) {}
  virtual void Visit(Query &query) = 0;
  virtual void PostVisit(Query &) {}
  // Expressions
  virtual void PreVisit(NamedExpression &) {}
  virtual void Visit(NamedExpression &) = 0;
  virtual void PostVisit(NamedExpression &) {}
  virtual void PreVisit(Identifier &) {}
  virtual void Visit(Identifier &ident) = 0;
  virtual void PostVisit(Identifier &) {}
  // Clauses
  virtual void PreVisit(Match &) {}
  virtual void Visit(Match &match) = 0;
  virtual void PostVisit(Match &) {}
  virtual void PreVisit(Return &) {}
  virtual void Visit(Return &ret) = 0;
  virtual void PostVisit(Return &) {}
  // Pattern and its subparts.
  virtual void PreVisit(Pattern &) {}
  virtual void Visit(Pattern &pattern) = 0;
  virtual void PostVisit(Pattern &) {}
  virtual void PreVisit(NodeAtom &) {}
  virtual void Visit(NodeAtom &node_part) = 0;
  virtual void PostVisit(NodeAtom &) {}
  virtual void PreVisit(EdgeAtom &) {}
  virtual void Visit(EdgeAtom &edge_part) = 0;
  virtual void PostVisit(EdgeAtom &) {}
};

}
