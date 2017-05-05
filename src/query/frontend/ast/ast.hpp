#pragma once

#include <map>
#include <memory>
#include <vector>

#include "database/graph_db.hpp"
#include "database/graph_db_datatypes.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/typed_value.hpp"
#include "utils/assert.hpp"
#include "utils/visitor/visitable.hpp"

namespace query {

class AstTreeStorage;

class Tree : public ::utils::Visitable<TreeVisitorBase> {
  friend class AstTreeStorage;

 public:
  int uid() const { return uid_; }

 protected:
  Tree(int uid) : uid_(uid) {}

 private:
  const int uid_;
};

class Expression : public Tree {
  friend class AstTreeStorage;

 protected:
  Expression(int uid) : Tree(uid) {}
};

class BinaryOperator : public Expression {
  friend class AstTreeStorage;

 public:
  Expression *expression1_ = nullptr;
  Expression *expression2_ = nullptr;

 protected:
  BinaryOperator(int uid) : Expression(uid) {}
  BinaryOperator(int uid, Expression *expression1, Expression *expression2)
      : Expression(uid), expression1_(expression1), expression2_(expression2) {}
};

class UnaryOperator : public Expression {
  friend class AstTreeStorage;

 public:
  Expression *expression_ = nullptr;

 protected:
  UnaryOperator(int uid) : Expression(uid) {}
  UnaryOperator(int uid, Expression *expression)
      : Expression(uid), expression_(expression) {}
};

class OrOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      // TODO: Should we short-circuit?
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class XorOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class AndOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      // TODO: Should we short-circuit?
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class AdditionOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class SubtractionOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class MultiplicationOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class DivisionOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class ModOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class NotEqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class EqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class LessOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class GreaterOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class LessEqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class GreaterEqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class ListIndexingOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression1_->Accept(visitor);
      expression2_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using BinaryOperator::BinaryOperator;
};

class ListSlicingOperator : public Expression {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      list_->Accept(visitor);
      if (lower_bound_) {
        lower_bound_->Accept(visitor);
      }
      if (upper_bound_) {
        upper_bound_->Accept(visitor);
      }
      visitor.PostVisit(*this);
    }
  }

  Expression *list_;
  Expression *lower_bound_;
  Expression *upper_bound_;

 protected:
  ListSlicingOperator(int uid, Expression *list, Expression *lower_bound,
                      Expression *upper_bound)
      : Expression(uid),
        list_(list),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound) {}
};

class NotOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using UnaryOperator::UnaryOperator;
};

class UnaryPlusOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using UnaryOperator::UnaryOperator;
};

class UnaryMinusOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  using UnaryOperator::UnaryOperator;
};

class IsNullOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    visitor.Visit(*this);
    expression_->Accept(visitor);
    visitor.PostVisit(*this);
  }

 protected:
  using UnaryOperator::UnaryOperator;
};

class BaseLiteral : public Expression {
  friend class AstTreeStorage;

 protected:
  BaseLiteral(int uid) : Expression(uid) {}
};

class PrimitiveLiteral : public BaseLiteral {
  friend class AstTreeStorage;

 public:
  TypedValue value_;
  DEFVISITABLE(TreeVisitorBase);

 protected:
  PrimitiveLiteral(int uid) : BaseLiteral(uid) {}
  template <typename T>
  PrimitiveLiteral(int uid, T value) : BaseLiteral(uid), value_(value) {}
};

class ListLiteral : public BaseLiteral {
  friend class AstTreeStorage;

 public:
  const std::vector<Expression *> elements_;
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      for (auto expr_ptr : elements_) expr_ptr->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

 protected:
  ListLiteral(int uid) : BaseLiteral(uid) {}
  ListLiteral(int uid, const std::vector<Expression *> &elements)
      : BaseLiteral(uid), elements_(elements) {}
};

class Identifier : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitorBase);
  std::string name_;
  bool user_declared_ = true;

 protected:
  Identifier(int uid, const std::string &name) : Expression(uid), name_(name) {}
  Identifier(int uid, const std::string &name, bool user_declared)
      : Expression(uid), name_(name), user_declared_(user_declared) {}
};

class PropertyLookup : public Expression {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

  Expression *expression_ = nullptr;
  GraphDbTypes::Property property_ = nullptr;
  // TODO potential problem: property lookups are allowed on both map literals
  // and records, but map literals have strings as keys and records have
  // GraphDbTypes::Property
  //
  // possible solution: store both string and GraphDbTypes::Property here and
  // choose
  // between the two depending on Expression result

 protected:
  PropertyLookup(int uid, Expression *expression,
                 GraphDbTypes::Property property)
      : Expression(uid), expression_(expression), property_(property) {}
};

class LabelsTest : public Expression {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

  Expression *expression_ = nullptr;
  std::vector<GraphDbTypes::Label> labels_;

 protected:
  LabelsTest(int uid, Expression *expression,
             std::vector<GraphDbTypes::Label> labels)
      : Expression(uid), expression_(expression), labels_(labels) {}
};

class EdgeTypeTest : public Expression {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

  Expression *expression_ = nullptr;
  std::vector<GraphDbTypes::EdgeType> edge_types_;

 protected:
  EdgeTypeTest(int uid, Expression *expression,
               std::vector<GraphDbTypes::EdgeType> edge_types)
      : Expression(uid), expression_(expression), edge_types_(edge_types) {
    debug_assert(edge_types.size(),
                 "EdgeTypeTest must have at least one edge_type");
  }
};

class Function : public Expression {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      for (auto *argument : arguments_) {
        argument->Accept(visitor);
      }
      visitor.PostVisit(*this);
    }
  }

  std::function<TypedValue(const std::vector<TypedValue> &, GraphDbAccessor &)>
      function_;
  std::vector<Expression *> arguments_;

 protected:
  Function(int uid, std::function<TypedValue(const std::vector<TypedValue> &,
                                             GraphDbAccessor &)>
                        function,
           const std::vector<Expression *> &arguments)
      : Expression(uid), function_(function), arguments_(arguments) {}
};

class Aggregation : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  enum class Op { COUNT, MIN, MAX, SUM, AVG };
  static const constexpr char *const kCount = "COUNT";
  static const constexpr char *const kMin = "MIN";
  static const constexpr char *const kMax = "MAX";
  static const constexpr char *const kSum = "SUM";
  static const constexpr char *const kAvg = "AVG";

  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      if (expression_) {
        expression_->Accept(visitor);
      }
      visitor.PostVisit(*this);
    }
  }
  Op op_;

 protected:
  Aggregation(int uid, Expression *expression, Op op)
      : UnaryOperator(uid, expression), op_(op) {
    // COUNT without expression denotes COUNT(*) in cypher.
    debug_assert(expression || op == Aggregation::Op::COUNT,
                 "All aggregations, except COUNT require expression");
  }
};

class NamedExpression : public Tree {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

  std::string name_;
  Expression *expression_ = nullptr;

 protected:
  NamedExpression(int uid) : Tree(uid) {}
  NamedExpression(int uid, const std::string &name) : Tree(uid), name_(name) {}
  NamedExpression(int uid, const std::string &name, Expression *expression)
      : Tree(uid), name_(name), expression_(expression) {}
};

class PatternAtom : public Tree {
  friend class AstTreeStorage;

 public:
  Identifier *identifier_ = nullptr;

 protected:
  PatternAtom(int uid) : Tree(uid) {}
  PatternAtom(int uid, Identifier *identifier)
      : Tree(uid), identifier_(identifier) {}
};

class NodeAtom : public PatternAtom {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      identifier_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

  std::vector<GraphDbTypes::Label> labels_;
  // TODO: change to unordered_map
  std::map<GraphDbTypes::Property, Expression *> properties_;

 protected:
  using PatternAtom::PatternAtom;
};

class EdgeAtom : public PatternAtom {
  friend class AstTreeStorage;

 public:
  // TODO change to IN, OUT, BOTH
  // LEFT/RIGHT is not clear especially when expansion will not
  // necessarily go from left to right
  enum class Direction { LEFT, RIGHT, BOTH };

  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      identifier_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

  Direction direction_ = Direction::BOTH;
  std::vector<GraphDbTypes::EdgeType> edge_types_;
  // TODO: change to unordered_map
  std::map<GraphDbTypes::Property, Expression *> properties_;

 protected:
  using PatternAtom::PatternAtom;
  EdgeAtom(int uid, Identifier *identifier, Direction direction)
      : PatternAtom(uid, identifier), direction_(direction) {}
};

class Clause : public Tree {
  friend class AstTreeStorage;

 public:
  Clause(int uid) : Tree(uid) {}
};

class Pattern : public Tree {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      for (auto &part : atoms_) {
        part->Accept(visitor);
      }
      visitor.PostVisit(*this);
    }
  }
  Identifier *identifier_ = nullptr;
  std::vector<PatternAtom *> atoms_;

 protected:
  Pattern(int uid) : Tree(uid) {}
};

class Query : public Tree {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      for (auto &clause : clauses_) {
        clause->Accept(visitor);
      }
      visitor.PostVisit(*this);
    }
  }
  std::vector<Clause *> clauses_;

 protected:
  Query(int uid) : Tree(uid) {}
};

class Create : public Clause {
  friend class AstTreeStorage;

 public:
  Create(int uid) : Clause(uid) {}
  std::vector<Pattern *> patterns_;
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      for (auto &pattern : patterns_) {
        pattern->Accept(visitor);
      }
      visitor.PostVisit(*this);
    }
  }
};

class Where : public Tree {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }
  Expression *expression_ = nullptr;

 protected:
  Where(int uid) : Tree(uid) {}
  Where(int uid, Expression *expression) : Tree(uid), expression_(expression) {}
};

class Match : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      for (auto &pattern : patterns_) {
        pattern->Accept(visitor);
      }
      if (where_) {
        where_->Accept(visitor);
      }
      visitor.PostVisit(*this);
    }
  }
  std::vector<Pattern *> patterns_;
  Where *where_ = nullptr;
  bool optional_ = false;

 protected:
  Match(int uid) : Clause(uid) {}
  Match(int uid, bool optional) : Clause(uid), optional_(optional) {}
};

/** @brief Defines the order for sorting values (ascending or descending). */
enum class Ordering { ASC, DESC };

/**
 * @brief Contents common to @c Return and @c With clauses.
 */
struct ReturnBody {
  /** @brief True if distinct results should be produced. */
  bool distinct = false;
  /** @brief True if asterisk was found in return body */
  bool all_identifiers = false;
  /** @brief Expressions which are used to produce results. */
  std::vector<NamedExpression *> named_expressions;
  /** @brief Expressions used for ordering the results. */
  std::vector<std::pair<Ordering, Expression *>> order_by;
  /** @brief Optional expression on how many results to skip. */
  Expression *skip = nullptr;
  /** @brief Optional expression on how much to limit the results. */
  Expression *limit = nullptr;
};

class Return : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      for (auto &expr : body_.named_expressions) {
        expr->Accept(visitor);
      }
      for (auto &order_by : body_.order_by) {
        order_by.second->Accept(visitor);
      }
      if (body_.skip) body_.skip->Accept(visitor);
      if (body_.limit) body_.limit->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

  ReturnBody body_;

 protected:
  Return(int uid) : Clause(uid) {}
};

class With : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      for (auto &expr : body_.named_expressions) {
        expr->Accept(visitor);
      }
      for (auto &order_by : body_.order_by) {
        order_by.second->Accept(visitor);
      }
      if (where_) where_->Accept(visitor);
      if (body_.skip) body_.skip->Accept(visitor);
      if (body_.limit) body_.limit->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

  ReturnBody body_;
  Where *where_ = nullptr;

 protected:
  With(int uid) : Clause(uid) {}
};

class Delete : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      for (auto &expr : expressions_) {
        expr->Accept(visitor);
      }
      visitor.PostVisit(*this);
    }
  }
  std::vector<Expression *> expressions_;
  bool detach_ = false;

 protected:
  Delete(int uid) : Clause(uid) {}
};

class SetProperty : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      property_lookup_->Accept(visitor);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }
  PropertyLookup *property_lookup_ = nullptr;
  Expression *expression_ = nullptr;

 protected:
  SetProperty(int uid) : Clause(uid) {}
  SetProperty(int uid, PropertyLookup *property_lookup, Expression *expression)
      : Clause(uid),
        property_lookup_(property_lookup),
        expression_(expression) {}
};

class SetProperties : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      identifier_->Accept(visitor);
      expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }
  Identifier *identifier_ = nullptr;
  Expression *expression_ = nullptr;
  bool update_ = false;

 protected:
  SetProperties(int uid) : Clause(uid) {}
  SetProperties(int uid, Identifier *identifier, Expression *expression,
                bool update = false)
      : Clause(uid),
        identifier_(identifier),
        expression_(expression),
        update_(update) {}
};

class SetLabels : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      identifier_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }
  Identifier *identifier_ = nullptr;
  std::vector<GraphDbTypes::Label> labels_;

 protected:
  SetLabels(int uid) : Clause(uid) {}
  SetLabels(int uid, Identifier *identifier,
            const std::vector<GraphDbTypes::Label> &labels)
      : Clause(uid), identifier_(identifier), labels_(labels) {}
};

class RemoveProperty : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      property_lookup_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }
  PropertyLookup *property_lookup_ = nullptr;

 protected:
  RemoveProperty(int uid) : Clause(uid) {}
  RemoveProperty(int uid, PropertyLookup *property_lookup)
      : Clause(uid), property_lookup_(property_lookup) {}
};

class RemoveLabels : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      identifier_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }
  Identifier *identifier_ = nullptr;
  std::vector<GraphDbTypes::Label> labels_;

 protected:
  RemoveLabels(int uid) : Clause(uid) {}
  RemoveLabels(int uid, Identifier *identifier,
               const std::vector<GraphDbTypes::Label> &labels)
      : Clause(uid), identifier_(identifier), labels_(labels) {}
};

class Merge : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      pattern_->Accept(visitor);
      for (auto &set : on_match_) {
        set->Accept(visitor);
      }
      for (auto &set : on_create_) {
        set->Accept(visitor);
      }
      visitor.PostVisit(*this);
    }
  }

  Pattern *pattern_ = nullptr;
  std::vector<Clause *> on_match_;
  std::vector<Clause *> on_create_;

 protected:
  Merge(int uid) : Clause(uid) {}
};

class Unwind : public Clause {
  friend class AstTreeStorage;

 public:
  void Accept(TreeVisitorBase &visitor) override {
    if (visitor.PreVisit(*this)) {
      visitor.Visit(*this);
      named_expression_->Accept(visitor);
      visitor.PostVisit(*this);
    }
  }

  NamedExpression *const named_expression_ = nullptr;

 protected:
  Unwind(int uid, NamedExpression *named_expression)
      : Clause(uid), named_expression_(named_expression) {
    debug_assert(named_expression,
                 "Unwind cannot take nullptr for named_expression")
  }
};

// It would be better to call this AstTree, but we already have a class Tree,
// which could be renamed to Node or AstTreeNode, but we also have a class
// called NodeAtom...
class AstTreeStorage {
 public:
  AstTreeStorage() { storage_.emplace_back(new Query(next_uid_++)); }
  AstTreeStorage(const AstTreeStorage &) = delete;
  AstTreeStorage &operator=(const AstTreeStorage &) = delete;

  template <typename T, typename... Args>
  T *Create(Args &&... args) {
    // Never call create for a Query. Call query() instead.
    static_assert(!std::is_same<T, Query>::value, "Call query() instead");
    T *p = new T(next_uid_++, std::forward<Args>(args)...);
    storage_.emplace_back(p);
    return p;
  }

  Query *query() { return dynamic_cast<Query *>(storage_[0].get()); }

 private:
  int next_uid_ = 0;
  std::vector<std::unique_ptr<Tree>> storage_;
};
}
