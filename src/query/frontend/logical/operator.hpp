#pragma once

#include <memory>
#include <sstream>
#include <vector>

#include "database/graph_db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/semantic/symbol_table.hpp"

namespace query {

class Cursor {
 public:
  virtual bool Pull(Frame&, SymbolTable&) = 0;
  virtual ~Cursor() {}
};

class LogicalOperator {
 public:
  auto children() { return children_; };
  virtual std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) = 0;
  virtual ~LogicalOperator() {}

 protected:
  std::vector<std::shared_ptr<LogicalOperator>> children_;
};

class CreateOp : public LogicalOperator {
public:
  CreateOp(NodeAtom* node_atom) : node_atom_(node_atom) {}

private:
  class CreateOpCursor : public Cursor {
  public:
    CreateOpCursor(CreateOp& self, GraphDbAccessor& db) : self_(self), db_(db) {}

    bool Pull(Frame &frame, SymbolTable &symbol_table) override {
      if (!did_create_) {
        auto new_node = db_.insert_vertex();
        for (auto label : self_.node_atom_->labels_)
          new_node.add_label(label);

        ExpressionEvaluator evaluator(frame, symbol_table);
        for (auto &kv : self_.node_atom_->properties_){
          kv.second->Accept(evaluator);
          new_node.PropsSet(kv.first, evaluator.PopBack());
        }

        did_create_ = true;
        return true;
      } else
        return false;
    }

  private:
    CreateOp &self_;
    GraphDbAccessor &db_;
    bool did_create_{false};
  };

public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<CreateOpCursor>(*this, db);
  }

private:
  NodeAtom* node_atom_ = nullptr;
};

class ScanAll : public LogicalOperator {
 public:
  ScanAll(NodeAtom *node_atom) : node_atom_(node_atom) {}

 private:
  class ScanAllCursor : public Cursor {
   public:
    ScanAllCursor(ScanAll& self, GraphDbAccessor& db)
        : self_(self),
          vertices_(db.vertices()),
          vertices_it_(vertices_.begin()) {}

    bool Pull(Frame& frame, SymbolTable& symbol_table) override {
      if (vertices_it_ == vertices_.end()) return false;
      frame[symbol_table[*self_.node_atom_->identifier_]] = *vertices_it_++;
      return true;
    }

   private:
    ScanAll& self_;
    decltype(std::declval<GraphDbAccessor>().vertices()) vertices_;
    decltype(vertices_.begin()) vertices_it_;
  };

 public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<ScanAllCursor>(*this, db);
  }

 private:
  NodeAtom *node_atom_ = nullptr;
};

class NodeFilter : public LogicalOperator {
 public:
  NodeFilter(
      std::shared_ptr<LogicalOperator> input, Symbol input_symbol,
      std::vector<GraphDb::Label> labels,
      std::map<GraphDb::Property, Expression*> properties)
      : input_(input),
        input_symbol_(input_symbol),
        labels_(labels),
        properties_(properties) {}

 private:
  class NodeFilterCursor : public Cursor {
   public:
    NodeFilterCursor(NodeFilter& self, GraphDbAccessor& db)
        : self_(self), input_cursor_(self_.input_->MakeCursor(db)) {}

    bool Pull(Frame& frame, SymbolTable& symbol_table) override {
      while (input_cursor_->Pull(frame, symbol_table)) {
        const auto &vertex = frame[self_.input_symbol_].Value<VertexAccessor>();
        if (VertexPasses(vertex, frame, symbol_table)) return true;
      }
      return false;
    }

   private:
    NodeFilter& self_;
    std::unique_ptr<Cursor> input_cursor_;

    bool VertexPasses(const VertexAccessor& vertex, Frame& frame,
                      SymbolTable& symbol_table) {
      for (auto label : self_.labels_)
        if (!vertex.has_label(label)) return false;

      ExpressionEvaluator expression_evaluator(frame, symbol_table);
      for (auto prop_pair : self_.properties_) {
        prop_pair.second->Accept(expression_evaluator);
        TypedValue comparison_result =
            vertex.PropsAt(prop_pair.first) == expression_evaluator.PopBack();
        if (comparison_result.type() == TypedValue::Type::Null ||
            !comparison_result.Value<bool>())
          return false;
      }
      return true;
    }
  };

 public:
  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<NodeFilterCursor>(*this, db);
  }

 private:
  std::shared_ptr<LogicalOperator> input_;
  const Symbol input_symbol_;
  std::vector<GraphDb::Label> labels_;
  std::map<GraphDb::Property, Expression*> properties_;
};

class Produce : public LogicalOperator {
 public:
  Produce(std::shared_ptr<LogicalOperator> input,
          std::vector<NamedExpression*> named_expressions)
      : input_(input), named_expressions_(named_expressions) {
    children_.emplace_back(input);
  }

  std::unique_ptr<Cursor> MakeCursor(GraphDbAccessor& db) override {
    return std::make_unique<ProduceCursor>(*this, db);
  }

  const auto& named_expressions() { return named_expressions_; }

 private:
  class ProduceCursor : public Cursor {
   public:
    ProduceCursor(Produce& self, GraphDbAccessor& db)
        : self_(self), self_cursor_(self_.input_->MakeCursor(db)) {}
    bool Pull(Frame& frame, SymbolTable& symbol_table) override {
      ExpressionEvaluator evaluator(frame, symbol_table);
      if (self_cursor_->Pull(frame, symbol_table)) {
        for (auto named_expr : self_.named_expressions_) {
          named_expr->Accept(evaluator);
        }
        return true;
      }
      return false;
    }

   private:
    Produce& self_;
    std::unique_ptr<Cursor> self_cursor_;
  };

 private:
  std::shared_ptr<LogicalOperator> input_;
  std::vector<NamedExpression*> named_expressions_;
};
}
