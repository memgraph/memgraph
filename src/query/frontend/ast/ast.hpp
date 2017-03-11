#pragma once

#include <memory>
#include <vector>

#include "database/graph_db.hpp"
#include "query/backend/cpp/typed_value.hpp"

namespace query {

class Frame;
class SymbolTable;

class Tree {
public:
  Tree(const int uid) : uid_(uid) {}
  int uid() const { return uid_; }

private:
  const int uid_;
};

class Expr : public Tree {
public:
  virtual TypedValue Evaluate(Frame &, SymbolTable &) = 0;
};

class Ident : public Expr {
public:
  std::string identifier_;
  TypedValue Evaluate(Frame &frame, SymbolTable &symbol_table) override;
};

class Part {};

class NodePart : public Part {
public:
  Ident identifier_;
  // TODO: Mislav call GraphDb::label(label_name) to populate labels_!
  std::vector<GraphDb::Label> labels_;
  // TODO: properties
};

class EdgePart : public Part {
public:
  Ident identifier_;
  // TODO: finish this: properties, types...
};

class Clause : public Tree {};

class Pattern : public Tree {
public:
  std::vector<std::unique_ptr<Part>> node_parts_;
};

class Query : public Tree {
public:
  std::vector<std::unique_ptr<Clause>> clauses_;
};

class Match : public Clause {
public:
  std::vector<std::unique_ptr<Pattern>> patterns_;
};

class Return : public Clause {
public:
  std::vector<std::unique_ptr<Expr>> exprs_;
};
}
