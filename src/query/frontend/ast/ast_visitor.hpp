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
  virtual void PreVisit(Query&) {}
  virtual void Visit(Query&) {}
  virtual void PostVisit(Query&) {}
  // Expressions
  virtual void PreVisit(NamedExpression&) {}
  virtual void Visit(NamedExpression&) {}
  virtual void PostVisit(NamedExpression&) {}
  virtual void PreVisit(Identifier&) {}
  virtual void Visit(Identifier&) {}
  virtual void PostVisit(Identifier&) {}
  // Clauses
  virtual void PreVisit(Match&) {}
  virtual void Visit(Match&) {}
  virtual void PostVisit(Match&) {}
  virtual void PreVisit(Return&) {}
  virtual void Visit(Return&) {}
  virtual void PostVisit(Return&) {}
  // Pattern and its subparts.
  virtual void PreVisit(Pattern&) {}
  virtual void Visit(Pattern&) {}
  virtual void PostVisit(Pattern&) {}
  virtual void PreVisit(NodeAtom&) {}
  virtual void Visit(NodeAtom&) {}
  virtual void PostVisit(NodeAtom&) {}
  virtual void PreVisit(EdgeAtom&) {}
  virtual void Visit(EdgeAtom&) {}
  virtual void PostVisit(EdgeAtom&) {}
};

}
