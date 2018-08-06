#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/typed_value.hpp"
#include "storage/types.hpp"
#include "utils/serialization.hpp"

#include "ast.capnp.h"

// Hash function for the key in pattern atom property maps.
namespace std {
template <>
struct hash<std::pair<std::string, storage::Property>> {
  size_t operator()(
      const std::pair<std::string, storage::Property> &pair) const {
    return string_hash(pair.first) ^ property_hash(pair.second);
  };

 private:
  std::hash<std::string> string_hash{};
  std::hash<storage::Property> property_hash{};
};
}  // namespace std

namespace database {
class GraphDbAccessor;
}

namespace query {

#define CLONE_BINARY_EXPRESSION                                              \
  auto Clone(AstStorage &storage) const->std::remove_const<                  \
      std::remove_pointer<decltype(this)>::type>::type *override {           \
    return storage.Create<                                                   \
        std::remove_cv<std::remove_reference<decltype(*this)>::type>::type>( \
        expression1_->Clone(storage), expression2_->Clone(storage));         \
  }
#define CLONE_UNARY_EXPRESSION                                               \
  auto Clone(AstStorage &storage) const->std::remove_const<                  \
      std::remove_pointer<decltype(this)>::type>::type *override {           \
    return storage.Create<                                                   \
        std::remove_cv<std::remove_reference<decltype(*this)>::type>::type>( \
        expression_->Clone(storage));                                        \
  }

class Context;
class Tree;

// It would be better to call this AstTree, but we already have a class Tree,
// which could be renamed to Node or AstTreeNode, but we also have a class
// called NodeAtom...
class AstStorage {
 public:
  AstStorage();
  AstStorage(const AstStorage &) = delete;
  AstStorage &operator=(const AstStorage &) = delete;
  AstStorage(AstStorage &&) = default;
  AstStorage &operator=(AstStorage &&) = default;

  template <typename T, typename... Args>
  T *Create(Args &&... args) {
    // Never call create for a Query. Call query() instead.
    static_assert(!std::is_same<T, Query>::value, "Call query() instead");
    T *p = new T(next_uid_++, std::forward<Args>(args)...);
    storage_.emplace_back(p);
    return p;
  }

  Query *query() const;

  Tree *Load(const capnp::Tree::Reader &tree, std::vector<int> *loaded_uids);

 private:
  int next_uid_ = 0;
  std::vector<std::unique_ptr<Tree>> storage_;
};

class Tree : public ::utils::Visitable<HierarchicalTreeVisitor>,
             ::utils::Visitable<TreeVisitor<TypedValue>> {
  friend class AstStorage;

 public:
  using ::utils::Visitable<HierarchicalTreeVisitor>::Accept;
  using ::utils::Visitable<TreeVisitor<TypedValue>>::Accept;

  int uid() const { return uid_; }

  virtual Tree *Clone(AstStorage &storage) const = 0;
  virtual void Save(capnp::Tree::Builder *builder,
                    std::vector<int> *saved_uids);

 protected:
  explicit Tree(int uid) : uid_(uid) {}

  virtual void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
                    std::vector<int> *loaded_uids);
  bool IsSaved(const std::vector<int> &saved_uids);
  void AddToSaved(std::vector<int> *saved_uids);

 private:
  int uid_;
};

// Expressions

class Expression : public Tree {
  friend class AstStorage;

 public:
  Expression *Clone(AstStorage &storage) const override = 0;
  static Expression *Construct(const capnp::Expression::Reader &reader,
                               AstStorage *storage);

  void Save(capnp::Tree::Builder *builder,
            std::vector<int> *saved_uids) override;

 protected:
  explicit Expression(int uid) : Tree(uid) {}

  virtual void Save(capnp::Expression::Builder *, std::vector<int> *) {}
};

class Where : public Tree {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Where *Clone(AstStorage &storage) const override {
    return storage.Create<Where>(expression_->Clone(storage));
  }

  static Where *Construct(const capnp::Where::Reader &reader,
                          AstStorage *storage);

  void Save(capnp::Tree::Builder *builder,
            std::vector<int> *saved_uids) override;

  Expression *expression_ = nullptr;

 protected:
  explicit Where(int uid) : Tree(uid) {}
  Where(int uid, Expression *expression) : Tree(uid), expression_(expression) {}

  virtual void Save(capnp::Where::Builder *, std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &tree_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class BinaryOperator : public Expression {
  friend class AstStorage;

 public:
  Expression *expression1_ = nullptr;
  Expression *expression2_ = nullptr;

  BinaryOperator *Clone(AstStorage &storage) const override = 0;
  static BinaryOperator *Construct(const capnp::BinaryOperator::Reader &reader,
                                   AstStorage *storage);

 protected:
  explicit BinaryOperator(int uid) : Expression(uid) {}
  BinaryOperator(int uid, Expression *expression1, Expression *expression2)
      : Expression(uid), expression1_(expression1), expression2_(expression2) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  using Expression::Save;
  virtual void Save(capnp::BinaryOperator::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class UnaryOperator : public Expression {
  friend class AstStorage;

 public:
  Expression *expression_ = nullptr;

  UnaryOperator *Clone(AstStorage &storage) const override = 0;
  static UnaryOperator *Construct(const capnp::UnaryOperator::Reader &reader,
                                  AstStorage *storage);

 protected:
  explicit UnaryOperator(int uid) : Expression(uid) {}
  UnaryOperator(int uid, Expression *expression)
      : Expression(uid), expression_(expression) {}

  void Save(capnp::Expression::Builder *,
            std::vector<int> *saved_uids) override;
  using Expression::Save;
  virtual void Save(capnp::UnaryOperator::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class OrOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

  static OrOperator *Construct(const capnp::OrOperator::Reader &reader,
                               AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class XorOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static XorOperator *Construct(const capnp::XorOperator::Reader &reader,
                                AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class AndOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static AndOperator *Construct(const capnp::AndOperator::Reader &reader,
                                AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class AdditionOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static AdditionOperator *Construct(
      const capnp::AdditionOperator::Reader &reader, AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class SubtractionOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static SubtractionOperator *Construct(
      capnp::SubtractionOperator::Reader &reader, AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class MultiplicationOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static MultiplicationOperator *Construct(
      capnp::MultiplicationOperator::Reader &reader, AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class DivisionOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static DivisionOperator *Construct(
      const capnp::DivisionOperator::Reader &reader, AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class ModOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static ModOperator *Construct(const capnp::ModOperator::Reader &reader,
                                AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class NotEqualOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static NotEqualOperator *Construct(
      const capnp::NotEqualOperator::Reader &reader, AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class EqualOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static EqualOperator *Construct(const capnp::EqualOperator::Reader &reader,
                                  AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class LessOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static LessOperator *Construct(const capnp::LessOperator::Reader &reader,
                                 AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class GreaterOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static GreaterOperator *Construct(
      const capnp::GreaterOperator::Reader &reader, AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class LessEqualOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static LessEqualOperator *Construct(
      const capnp::LessEqualOperator::Reader &reader, AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class GreaterEqualOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static GreaterEqualOperator *Construct(
      const capnp::GreaterEqualOperator::Reader &reader, AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class InListOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static InListOperator *Construct(const capnp::InListOperator::Reader &reader,
                                   AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class SubscriptOperator : public BinaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;
  static SubscriptOperator *Construct(capnp::SubscriptOperator::Reader &reader,
                                      AstStorage *storage);

 protected:
  using BinaryOperator::BinaryOperator;
  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class ListSlicingOperator : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = list_->Accept(visitor);
      if (cont && lower_bound_) {
        cont = lower_bound_->Accept(visitor);
      }
      if (cont && upper_bound_) {
        upper_bound_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  ListSlicingOperator *Clone(AstStorage &storage) const override {
    return storage.Create<ListSlicingOperator>(
        list_->Clone(storage),
        lower_bound_ ? lower_bound_->Clone(storage) : nullptr,
        upper_bound_ ? upper_bound_->Clone(storage) : nullptr);
  }

  static ListSlicingOperator *Construct(
      const capnp::ListSlicingOperator::Reader &reader, AstStorage *storage);

  Expression *list_ = nullptr;
  Expression *lower_bound_ = nullptr;
  Expression *upper_bound_ = nullptr;

 protected:
  ListSlicingOperator(int uid, Expression *list, Expression *lower_bound,
                      Expression *upper_bound)
      : Expression(uid),
        list_(list),
        lower_bound_(lower_bound),
        upper_bound_(upper_bound) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  using Expression::Save;
  virtual void Save(capnp::ListSlicingOperator::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class IfOperator : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      condition_->Accept(visitor) && then_expression_->Accept(visitor) &&
          else_expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  IfOperator *Clone(AstStorage &storage) const override {
    return storage.Create<IfOperator>(condition_->Clone(storage),
                                      then_expression_->Clone(storage),
                                      else_expression_->Clone(storage));
  }

  static IfOperator *Construct(const capnp::IfOperator::Reader &reader,
                               AstStorage *storage);
  // None of the expressions should be nullptrs. If there is no else_expression
  // you probably want to make it NULL PrimitiveLiteral.
  Expression *condition_;
  Expression *then_expression_;
  Expression *else_expression_;

 protected:
  IfOperator(int uid, Expression *condition, Expression *then_expression,
             Expression *else_expression)
      : Expression(uid),
        condition_(condition),
        then_expression_(then_expression),
        else_expression_(else_expression) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  using Expression::Save;
  virtual void Save(capnp::IfOperator::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class NotOperator : public UnaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;
  static NotOperator *Construct(const capnp::NotOperator::Reader &reader,
                                AstStorage *storage);

 protected:
  using UnaryOperator::UnaryOperator;
  void Save(capnp::UnaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class UnaryPlusOperator : public UnaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;
  static UnaryPlusOperator *Construct(
      const capnp::UnaryPlusOperator::Reader &reader, AstStorage *storage);

 protected:
  using UnaryOperator::UnaryOperator;
  void Save(capnp::UnaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class UnaryMinusOperator : public UnaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;
  static UnaryMinusOperator *Construct(
      capnp::UnaryMinusOperator::Reader &reader, AstStorage *storage);

 protected:
  using UnaryOperator::UnaryOperator;
  void Save(capnp::UnaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class IsNullOperator : public UnaryOperator {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;
  static IsNullOperator *Construct(const capnp::IsNullOperator::Reader &reader,
                                   AstStorage *storage);

 protected:
  using UnaryOperator::UnaryOperator;
  void Save(capnp::UnaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class BaseLiteral : public Expression {
  friend class AstStorage;

 public:
  BaseLiteral *Clone(AstStorage &storage) const override = 0;
  static BaseLiteral *Construct(const capnp::BaseLiteral::Reader &reader,
                                AstStorage *storage);

 protected:
  explicit BaseLiteral(int uid) : Expression(uid) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  using Expression::Save;
  virtual void Save(capnp::BaseLiteral::Builder *,
                    std::vector<int> *saved_uids) {}
};

class PrimitiveLiteral : public BaseLiteral {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  PrimitiveLiteral *Clone(AstStorage &storage) const override {
    return storage.Create<PrimitiveLiteral>(value_, token_position_);
  }

  static PrimitiveLiteral *Construct(
      const capnp::PrimitiveLiteral::Reader &reader, AstStorage *storage);

  TypedValue value_;
  // This field contains token position of literal used to create
  // PrimitiveLiteral object. If PrimitiveLiteral object is not created from
  // query leave its value at -1.
  int token_position_ = -1;

 protected:
  explicit PrimitiveLiteral(int uid) : BaseLiteral(uid) {}
  template <typename T>
  PrimitiveLiteral(int uid, T value) : BaseLiteral(uid), value_(value) {}
  template <typename T>
  PrimitiveLiteral(int uid, T value, int token_position)
      : BaseLiteral(uid), value_(value), token_position_(token_position) {}

  void Save(capnp::BaseLiteral::Builder *builder,
            std::vector<int> *saved_uids) override;
  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class ListLiteral : public BaseLiteral {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto expr_ptr : elements_)
        if (!expr_ptr->Accept(visitor)) break;
    }
    return visitor.PostVisit(*this);
  }

  ListLiteral *Clone(AstStorage &storage) const override {
    auto *list = storage.Create<ListLiteral>();
    for (auto *element : elements_) {
      list->elements_.push_back(element->Clone(storage));
    }
    return list;
  }

  static ListLiteral *Construct(const capnp::ListLiteral::Reader &reader,
                                AstStorage *storage);

  std::vector<Expression *> elements_;

 protected:
  explicit ListLiteral(int uid) : BaseLiteral(uid) {}
  ListLiteral(int uid, const std::vector<Expression *> &elements)
      : BaseLiteral(uid), elements_(elements) {}

  void Save(capnp::BaseLiteral::Builder *builder,
            std::vector<int> *saved_uids) override;
  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class MapLiteral : public BaseLiteral {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto pair : elements_)
        if (!pair.second->Accept(visitor)) break;
    }
    return visitor.PostVisit(*this);
  }

  MapLiteral *Clone(AstStorage &storage) const override {
    auto *map = storage.Create<MapLiteral>();
    for (auto pair : elements_)
      map->elements_.emplace(pair.first, pair.second->Clone(storage));
    return map;
  }

  static MapLiteral *Construct(const capnp::MapLiteral::Reader &reader,
                               AstStorage *storage);

  // maps (property_name, property) to expressions
  std::unordered_map<std::pair<std::string, storage::Property>, Expression *>
      elements_;

 protected:
  explicit MapLiteral(int uid) : BaseLiteral(uid) {}
  MapLiteral(int uid,
             const std::unordered_map<std::pair<std::string, storage::Property>,
                                      Expression *> &elements)
      : BaseLiteral(uid), elements_(elements) {}

  void Save(capnp::BaseLiteral::Builder *builder,
            std::vector<int> *saved_uids) override;
  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class Identifier : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  Identifier *Clone(AstStorage &storage) const override {
    return storage.Create<Identifier>(name_, user_declared_);
  }

  static Identifier *Construct(const capnp::Identifier::Reader &reader,
                               AstStorage *storage);
  using Expression::Save;

  std::string name_;
  bool user_declared_ = true;

 protected:
  Identifier(int uid, const std::string &name) : Expression(uid), name_(name) {}
  Identifier(int uid, const std::string &name, bool user_declared)
      : Expression(uid), name_(name), user_declared_(user_declared) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Identifier::Builder *builder,
                    std::vector<int> *saved_uids);
};

class PropertyLookup : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  PropertyLookup *Clone(AstStorage &storage) const override {
    return storage.Create<PropertyLookup>(expression_->Clone(storage),
                                          property_name_, property_);
  }

  static PropertyLookup *Construct(const capnp::PropertyLookup::Reader &reader,
                                   AstStorage *storage);
  using Expression::Save;

  Expression *expression_ = nullptr;
  std::string property_name_;
  storage::Property property_;

 protected:
  PropertyLookup(int uid, Expression *expression,
                 const std::string &property_name, storage::Property property)
      : Expression(uid),
        expression_(expression),
        property_name_(property_name),
        property_(property) {}
  PropertyLookup(int uid, Expression *expression,
                 const std::pair<std::string, storage::Property> &property)
      : Expression(uid),
        expression_(expression),
        property_name_(property.first),
        property_(property.second) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::PropertyLookup::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class LabelsTest : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  LabelsTest *Clone(AstStorage &storage) const override {
    return storage.Create<LabelsTest>(expression_->Clone(storage), labels_);
  }

  static LabelsTest *Construct(const capnp::LabelsTest::Reader &reader,
                               AstStorage *storage);
  using Expression::Save;

  Expression *expression_ = nullptr;
  std::vector<storage::Label> labels_;

 protected:
  LabelsTest(int uid, Expression *expression,
             const std::vector<storage::Label> &labels)
      : Expression(uid), expression_(expression), labels_(labels) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::LabelsTest::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &tree_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class Function : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto *argument : arguments_) {
        if (!argument->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  Function *Clone(AstStorage &storage) const override {
    std::vector<Expression *> arguments;
    for (auto *argument : arguments_) {
      arguments.push_back(argument->Clone(storage));
    }
    return storage.Create<Function>(function_name_, arguments);
  }

  static Function *Construct(const capnp::Function::Reader &reader,
                             AstStorage *storage);

  const auto &function() const { return function_; }
  const auto &function_name() const { return function_name_; }
  std::vector<Expression *> arguments_;

 protected:
  explicit Function(int uid) : Expression(uid) {}

  Function(int uid, const std::string &function_name,
           const std::vector<Expression *> &arguments)
      : Expression(uid),
        arguments_(arguments),
        function_name_(function_name),
        function_(NameToFunction(function_name_)) {
    DCHECK(function_) << "Unexpected missing function: " << function_name_;
  }
  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  using Expression::Save;
  virtual void Save(capnp::Function::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &tree_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;

 private:
  std::string function_name_;
  std::function<TypedValue(const std::vector<TypedValue> &, Context *)>
      function_;
};

class Aggregation : public BinaryOperator {
  friend class AstStorage;

 public:
  enum class Op { COUNT, MIN, MAX, SUM, AVG, COLLECT_LIST, COLLECT_MAP };
  static const constexpr char *const kCount = "COUNT";
  static const constexpr char *const kMin = "MIN";
  static const constexpr char *const kMax = "MAX";
  static const constexpr char *const kSum = "SUM";
  static const constexpr char *const kAvg = "AVG";
  static const constexpr char *const kCollect = "COLLECT";

  static std::string OpToString(Op op) {
    const char *op_strings[] = {kCount, kMin,     kMax,    kSum,
                                kAvg,   kCollect, kCollect};
    return op_strings[static_cast<int>(op)];
  }

  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      if (expression1_) expression1_->Accept(visitor);
      if (expression2_) expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Aggregation *Clone(AstStorage &storage) const override {
    return storage.Create<Aggregation>(
        expression1_ ? expression1_->Clone(storage) : nullptr,
        expression2_ ? expression2_->Clone(storage) : nullptr, op_);
  }

  Op op_;
  static Aggregation *Construct(const capnp::Aggregation::Reader &,
                                AstStorage *storage);

 protected:
  // Use only for serialization.
  Aggregation(int uid, Op op) : BinaryOperator(uid), op_(op) {}

  /// Aggregation's first expression is the value being aggregated. The second
  /// expression is the key used only in COLLECT_MAP.
  Aggregation(int uid, Expression *expression1, Expression *expression2, Op op)
      : BinaryOperator(uid, expression1, expression2), op_(op) {
    // COUNT without expression denotes COUNT(*) in cypher.
    DCHECK(expression1 || op == Aggregation::Op::COUNT)
        << "All aggregations, except COUNT require expression";
    DCHECK((expression2 == nullptr) ^ (op == Aggregation::Op::COLLECT_MAP))
        << "The second expression is obligatory in COLLECT_MAP and "
           "invalid otherwise";
  }

  void Save(capnp::BinaryOperator::Builder *builder,
            std::vector<int> *saved_uids) override;
};

class Reduce : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      accumulator_->Accept(visitor) && initializer_->Accept(visitor) &&
          identifier_->Accept(visitor) && list_->Accept(visitor) &&
          expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Reduce *Clone(AstStorage &storage) const override {
    return storage.Create<Reduce>(
        accumulator_->Clone(storage), initializer_->Clone(storage),
        identifier_->Clone(storage), list_->Clone(storage),
        expression_->Clone(storage));
  }

  static Reduce *Construct(const capnp::Reduce::Reader &reader,
                           AstStorage *storage);
  using Expression::Save;

  // None of these should be nullptr after construction.

  /// Identifier for the accumulating variable
  Identifier *accumulator_ = nullptr;
  /// Expression which produces the initial accumulator value.
  Expression *initializer_ = nullptr;
  /// Identifier for the list element.
  Identifier *identifier_ = nullptr;
  /// Expression which produces a list which will be reduced.
  Expression *list_ = nullptr;
  /// Expression which does the reduction, i.e. produces the new accumulator
  /// value.
  Expression *expression_ = nullptr;

 protected:
  Reduce(int uid, Identifier *accumulator, Expression *initializer,
         Identifier *identifier, Expression *list, Expression *expression)
      : Expression(uid),
        accumulator_(accumulator),
        initializer_(initializer),
        identifier_(identifier),
        list_(list),
        expression_(expression) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Reduce::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &tree_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class Extract : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_->Accept(visitor) &&
          expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Extract *Clone(AstStorage &storage) const override {
    return storage.Create<Extract>(identifier_->Clone(storage),
                                   list_->Clone(storage),
                                   expression_->Clone(storage));
  }

  static Extract *Construct(const capnp::Extract::Reader &reader,
                            AstStorage *storage);
  using Expression::Save;

  // None of these should be nullptr after construction.

  /// Identifier for the list element.
  Identifier *identifier_ = nullptr;
  /// Expression which produces a list which will be extracted.
  Expression *list_ = nullptr;
  /// Expression which produces the new value for list element.
  Expression *expression_ = nullptr;

 protected:
  Extract(int uid, Identifier *identifier, Expression *list,
          Expression *expression)
      : Expression(uid),
        identifier_(identifier),
        list_(list),
        expression_(expression) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Extract::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &tree_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

// TODO: Think about representing All and Any as Reduce.
class All : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_expression_->Accept(visitor) &&
          where_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  All *Clone(AstStorage &storage) const override {
    return storage.Create<All>(identifier_->Clone(storage),
                               list_expression_->Clone(storage),
                               where_->Clone(storage));
  }

  static All *Construct(const capnp::All::Reader &reader, AstStorage *storage);

  // None of these should be nullptr after construction.
  Identifier *identifier_ = nullptr;
  Expression *list_expression_ = nullptr;
  Where *where_ = nullptr;

 protected:
  All(int uid, Identifier *identifier, Expression *list_expression,
      Where *where)
      : Expression(uid),
        identifier_(identifier),
        list_expression_(list_expression),
        where_(where) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  using Expression::Save;
  virtual void Save(capnp::All::Builder *builder, std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &tree_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

// TODO: This is pretty much copy pasted from All. Consider merging Reduce,
// All, Any and Single into something like a higher-order function call which
// takes a list argument and a function which is applied on list elements.
class Single : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_expression_->Accept(visitor) &&
          where_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Single *Clone(AstStorage &storage) const override {
    return storage.Create<Single>(identifier_->Clone(storage),
                                  list_expression_->Clone(storage),
                                  where_->Clone(storage));
  }

  static Single *Construct(const capnp::Single::Reader &reader,
                           AstStorage *storage);
  using Expression::Save;

  // None of these should be nullptr after construction.
  Identifier *identifier_ = nullptr;
  Expression *list_expression_ = nullptr;
  Where *where_ = nullptr;

 protected:
  Single(int uid, Identifier *identifier, Expression *list_expression,
         Where *where)
      : Expression(uid),
        identifier_(identifier),
        list_expression_(list_expression),
        where_(where) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Single::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &tree_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class ParameterLookup : public Expression {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  ParameterLookup *Clone(AstStorage &storage) const override {
    return storage.Create<ParameterLookup>(token_position_);
  }

  static ParameterLookup *Construct(
      const capnp::ParameterLookup::Reader &reader, AstStorage *storage);
  using Expression::Save;

  // This field contains token position of *literal* used to create
  // ParameterLookup object. If ParameterLookup object is not created from
  // a literal leave this value at -1.
  int token_position_ = -1;

 protected:
  explicit ParameterLookup(int uid) : Expression(uid) {}
  ParameterLookup(int uid, int token_position)
      : Expression(uid), token_position_(token_position) {}

  void Save(capnp::Expression::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::ParameterLookup::Builder *builder,
                    std::vector<int> *saved_uids);
};

class NamedExpression : public Tree {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  NamedExpression *Clone(AstStorage &storage) const override {
    return storage.Create<NamedExpression>(name_, expression_->Clone(storage),
                                           token_position_);
  }

  static NamedExpression *Construct(
      const capnp::NamedExpression::Reader &reader, AstStorage *storage);
  void Save(capnp::Tree::Builder *builder,
            std::vector<int> *saved_uids) override;

  std::string name_;
  Expression *expression_ = nullptr;
  // This field contains token position of first token in named expression
  // used to create name_. If NamedExpression object is not created from
  // query or it is aliased leave this value at -1.
  int token_position_ = -1;

 protected:
  explicit NamedExpression(int uid) : Tree(uid) {}
  NamedExpression(int uid, const std::string &name) : Tree(uid), name_(name) {}
  NamedExpression(int uid, const std::string &name, Expression *expression)
      : Tree(uid), name_(name), expression_(expression) {}
  NamedExpression(int uid, const std::string &name, Expression *expression,
                  int token_position)
      : Tree(uid),
        name_(name),
        expression_(expression),
        token_position_(token_position) {}

  virtual void Save(capnp::NamedExpression::Builder *,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

// Pattern atoms

class PatternAtom : public Tree {
  friend class AstStorage;

 public:
  Identifier *identifier_ = nullptr;

  PatternAtom *Clone(AstStorage &storage) const override = 0;

  static PatternAtom *Construct(const capnp::PatternAtom::Reader &reader,
                                AstStorage *storage);
  void Save(capnp::Tree::Builder *builder,
            std::vector<int> *saved_uids) override;

 protected:
  explicit PatternAtom(int uid) : Tree(uid) {}
  PatternAtom(int uid, Identifier *identifier)
      : Tree(uid), identifier_(identifier) {}

  virtual void Save(capnp::PatternAtom::Builder *,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class NodeAtom : public PatternAtom {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = identifier_->Accept(visitor);
      for (auto &property : properties_) {
        if (cont) {
          cont = property.second->Accept(visitor);
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  NodeAtom *Clone(AstStorage &storage) const override {
    auto *node_atom = storage.Create<NodeAtom>(identifier_->Clone(storage));
    node_atom->labels_ = labels_;
    for (auto property : properties_) {
      node_atom->properties_[property.first] = property.second->Clone(storage);
    }
    return node_atom;
  }

  static NodeAtom *Construct(const capnp::NodeAtom::Reader &reader,
                             AstStorage *storage);
  using PatternAtom::Save;

  std::vector<storage::Label> labels_;
  // maps (property_name, property) to an expression
  std::unordered_map<std::pair<std::string, storage::Property>, Expression *>
      properties_;

 protected:
  using PatternAtom::PatternAtom;

  void Save(capnp::PatternAtom::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::NodeAtom::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class EdgeAtom : public PatternAtom {
  friend class AstStorage;

  template <typename TPtr>
  static TPtr *CloneOpt(TPtr *ptr, AstStorage &storage) {
    return ptr ? ptr->Clone(storage) : nullptr;
  }

 public:
  enum class Type {
    SINGLE,
    DEPTH_FIRST,
    BREADTH_FIRST,
    WEIGHTED_SHORTEST_PATH
  };
  enum class Direction { IN, OUT, BOTH };

  /// Lambda for use in filtering or weight calculation during variable
  /// expands.
  struct Lambda {
    /// Argument identifier for the edge currently being traversed.
    Identifier *inner_edge = nullptr;
    /// Argument identifier for the destination node of the edge.
    Identifier *inner_node = nullptr;
    /// Evaluates the result of the lambda.
    Expression *expression = nullptr;
  };

  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = identifier_->Accept(visitor);
      for (auto &property : properties_) {
        if (cont) {
          cont = property.second->Accept(visitor);
        }
      }
      if (cont && lower_bound_) {
        cont = lower_bound_->Accept(visitor);
      }
      if (cont && upper_bound_) {
        cont = upper_bound_->Accept(visitor);
      }
      if (cont && total_weight_) {
        total_weight_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  EdgeAtom *Clone(AstStorage &storage) const override {
    auto *edge_atom = storage.Create<EdgeAtom>(identifier_->Clone(storage));
    edge_atom->direction_ = direction_;
    edge_atom->type_ = type_;
    edge_atom->edge_types_ = edge_types_;
    for (auto property : properties_) {
      edge_atom->properties_[property.first] = property.second->Clone(storage);
    }
    edge_atom->lower_bound_ = CloneOpt(lower_bound_, storage);
    edge_atom->upper_bound_ = CloneOpt(upper_bound_, storage);
    auto clone_lambda = [&storage](const auto &lambda) {
      return Lambda{CloneOpt(lambda.inner_edge, storage),
                    CloneOpt(lambda.inner_node, storage),
                    CloneOpt(lambda.expression, storage)};
    };
    edge_atom->filter_lambda_ = clone_lambda(filter_lambda_);
    edge_atom->weight_lambda_ = clone_lambda(weight_lambda_);
    edge_atom->total_weight_ = CloneOpt(total_weight_, storage);
    return edge_atom;
  }

  bool IsVariable() const {
    switch (type_) {
      case Type::DEPTH_FIRST:
      case Type::BREADTH_FIRST:
      case Type::WEIGHTED_SHORTEST_PATH:
        return true;
      case Type::SINGLE:
        return false;
    }
  }

  static EdgeAtom *Construct(const capnp::EdgeAtom::Reader &reader,
                             AstStorage *storage);
  using PatternAtom::Save;

  Type type_ = Type::SINGLE;
  Direction direction_ = Direction::BOTH;
  std::vector<storage::EdgeType> edge_types_;
  std::unordered_map<std::pair<std::string, storage::Property>, Expression *>
      properties_;

  // Members used only in variable length expansions.

  /// Evaluates to lower bound in variable length expands.
  Expression *lower_bound_ = nullptr;
  /// Evaluated to upper bound in variable length expands.
  Expression *upper_bound_ = nullptr;
  /// Filter lambda for variable length expands.
  /// Can have an empty expression, but identifiers must be valid, because an
  /// optimization pass may inline other expressions into this lambda.
  Lambda filter_lambda_;
  /// Used in weighted shortest path.
  /// It must have valid expressions and identifiers. In all other expand
  /// types, it is empty.
  Lambda weight_lambda_;
  /// Variable where the total weight for weighted shortest path will be
  /// stored.
  Identifier *total_weight_ = nullptr;

 protected:
  using PatternAtom::PatternAtom;
  EdgeAtom(int uid, Identifier *identifier, Type type, Direction direction)
      : PatternAtom(uid, identifier), type_(type), direction_(direction) {}

  // Creates an edge atom for a SINGLE expansion with the given .
  EdgeAtom(int uid, Identifier *identifier, Type type, Direction direction,
           const std::vector<storage::EdgeType> &edge_types)
      : PatternAtom(uid, identifier),
        type_(type),
        direction_(direction),
        edge_types_(edge_types) {}

  void Save(capnp::PatternAtom::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::EdgeAtom::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class Pattern : public Tree {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = identifier_->Accept(visitor);
      for (auto &part : atoms_) {
        if (cont) {
          cont = part->Accept(visitor);
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  Pattern *Clone(AstStorage &storage) const override {
    auto *pattern = storage.Create<Pattern>();
    pattern->identifier_ = identifier_->Clone(storage);
    for (auto *atom : atoms_) {
      pattern->atoms_.push_back(atom->Clone(storage));
    }
    return pattern;
  }

  static Pattern *Construct(const capnp::Pattern::Reader &reader,
                            AstStorage *storage);
  void Save(capnp::Tree::Builder *builder,
            std::vector<int> *saved_uids) override;

  Identifier *identifier_ = nullptr;
  std::vector<PatternAtom *> atoms_;

 protected:
  explicit Pattern(int uid) : Tree(uid) {}

  virtual void Save(capnp::Pattern::Builder *, std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

// Clause

class Clause : public Tree {
  friend class AstStorage;

 public:
  explicit Clause(int uid) : Tree(uid) {}

  Clause *Clone(AstStorage &storage) const override = 0;

  static Clause *Construct(const capnp::Clause::Reader &reader,
                           AstStorage *storage);

  void Save(capnp::Tree::Builder *builder,
            std::vector<int> *saved_uids) override;

 protected:
  virtual void Save(capnp::Clause::Builder *, std::vector<int> *saved_uids) {}
};

// SingleQuery

class SingleQuery : public Tree {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &clause : clauses_) {
        if (!clause->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  SingleQuery *Clone(AstStorage &storage) const override {
    auto *single_query = storage.Create<SingleQuery>();
    for (auto *clause : clauses_) {
      single_query->clauses_.push_back(clause->Clone(storage));
    }
    return single_query;
  }

  static SingleQuery *Construct(const capnp::SingleQuery::Reader &reader,
                                AstStorage *storage);

  void Save(capnp::Tree::Builder *builder,
            std::vector<int> *saved_uids) override;

  std::vector<Clause *> clauses_;

 protected:
  explicit SingleQuery(int uid) : Tree(uid) {}

  virtual void Save(capnp::SingleQuery::Builder *,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

// CypherUnion

class CypherUnion : public Tree {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      single_query_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  CypherUnion *Clone(AstStorage &storage) const override {
    auto cypher_union = storage.Create<CypherUnion>(distinct_);
    cypher_union->single_query_ = single_query_->Clone(storage);
    cypher_union->union_symbols_ = union_symbols_;
    return cypher_union;
  }

  static CypherUnion *Construct(const capnp::CypherUnion::Reader &reader,
                                AstStorage *storage);
  void Save(capnp::Tree::Builder *builder,
            std::vector<int> *saved_uids) override;

  SingleQuery *single_query_ = nullptr;
  bool distinct_ = false;
  /// Holds symbols that are created during symbol generation phase.
  /// These symbols are used when UNION/UNION ALL combines single query
  /// results.
  std::vector<Symbol> union_symbols_;

 protected:
  explicit CypherUnion(int uid) : Tree(uid) {}
  CypherUnion(int uid, bool distinct) : Tree(uid), distinct_(distinct) {}
  CypherUnion(int uid, bool distinct, SingleQuery *single_query,
              std::vector<Symbol> union_symbols)
      : Tree(uid),
        single_query_(single_query),
        distinct_(distinct),
        union_symbols_(union_symbols) {}

  virtual void Save(capnp::CypherUnion::Builder *,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

// Queries

class Query : public Tree {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool should_continue = single_query_->Accept(visitor);
      for (auto *cypher_union : cypher_unions_) {
        if (should_continue) {
          should_continue = cypher_union->Accept(visitor);
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  // Creates deep copy of whole ast.
  Query *Clone(AstStorage &storage) const override {
    auto *query = storage.query();
    query->single_query_ = single_query_->Clone(storage);
    for (auto *cypher_union : cypher_unions_) {
      query->cypher_unions_.push_back(cypher_union->Clone(storage));
    }
    return query;
  }

  void Load(const capnp::Tree::Reader &reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
  void Save(capnp::Tree::Builder *builder,
            std::vector<int> *saved_uids) override;

  SingleQuery *single_query_ = nullptr;
  std::vector<CypherUnion *> cypher_unions_;

 protected:
  explicit Query(int uid) : Tree(uid) {}

  virtual void Save(capnp::Query::Builder *, std::vector<int> *saved_uids);
};

// Clauses

class Create : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &pattern : patterns_) {
        if (!pattern->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  Create *Clone(AstStorage &storage) const override {
    auto *create = storage.Create<Create>();
    for (auto *pattern : patterns_) {
      create->patterns_.push_back(pattern->Clone(storage));
    }
    return create;
  }

  static Create *Construct(const capnp::Create::Reader &reader,
                           AstStorage *storage);
  using Clause::Save;

  std::vector<Pattern *> patterns_;

 protected:
  explicit Create(int uid) : Clause(uid) {}
  Create(int uid, std::vector<Pattern *> patterns)
      : Clause(uid), patterns_(patterns) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Create::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &tree_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class Match : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &pattern : patterns_) {
        if (!pattern->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont && where_) {
        where_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  Match *Clone(AstStorage &storage) const override {
    auto *match = storage.Create<Match>(optional_);
    for (auto *pattern : patterns_) {
      match->patterns_.push_back(pattern->Clone(storage));
    }
    match->where_ = where_ ? where_->Clone(storage) : nullptr;
    return match;
  }

  using Clause::Save;
  static Match *Construct(const capnp::Match::Reader &reader,
                          AstStorage *storage);

  std::vector<Pattern *> patterns_;
  Where *where_ = nullptr;
  bool optional_ = false;

 protected:
  explicit Match(int uid) : Clause(uid) {}
  Match(int uid, bool optional) : Clause(uid), optional_(optional) {}
  Match(int uid, bool optional, Where *where, std::vector<Pattern *> patterns)
      : Clause(uid), patterns_(patterns), where_(where), optional_(optional) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Match::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

/// Defines the order for sorting values (ascending or descending).
enum class Ordering { ASC, DESC };

/// Contents common to @c Return and @c With clauses.
struct ReturnBody {
  /// True if distinct results should be produced.
  bool distinct = false;
  /// True if asterisk was found in return body
  bool all_identifiers = false;
  /// Expressions which are used to produce results.
  std::vector<NamedExpression *> named_expressions;
  /// Expressions used for ordering the results.
  std::vector<std::pair<Ordering, Expression *>> order_by;
  /// Optional expression on how many results to skip.
  Expression *skip = nullptr;
  /// Optional expression on how much to limit the results.
  Expression *limit = nullptr;
};

// Deep copy ReturnBody.
// TODO: Think about turning ReturnBody to class and making this
// function class member.
ReturnBody CloneReturnBody(AstStorage &storage, const ReturnBody &body);

class Return : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &expr : body_.named_expressions) {
        if (!expr->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont) {
        for (auto &order_by : body_.order_by) {
          if (!order_by.second->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
      if (cont && body_.skip) cont = body_.skip->Accept(visitor);
      if (cont && body_.limit) cont = body_.limit->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Return *Clone(AstStorage &storage) const override {
    auto *ret = storage.Create<Return>();
    ret->body_ = CloneReturnBody(storage, body_);
    return ret;
  }

  using Clause::Save;
  static Return *Construct(const capnp::Return::Reader &reader,
                           AstStorage *storage);

  ReturnBody body_;

 protected:
  explicit Return(int uid) : Clause(uid) {}
  Return(int uid, ReturnBody &body) : Clause(uid), body_(body) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Return::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class With : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = true;
      for (auto &expr : body_.named_expressions) {
        if (!expr->Accept(visitor)) {
          cont = false;
          break;
        }
      }
      if (cont) {
        for (auto &order_by : body_.order_by) {
          if (!order_by.second->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
      if (cont && where_) cont = where_->Accept(visitor);
      if (cont && body_.skip) cont = body_.skip->Accept(visitor);
      if (cont && body_.limit) cont = body_.limit->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  With *Clone(AstStorage &storage) const override {
    auto *with = storage.Create<With>();
    with->body_ = CloneReturnBody(storage, body_);
    with->where_ = where_ ? where_->Clone(storage) : nullptr;
    return with;
  }

  using Clause::Save;
  static With *Construct(const capnp::With::Reader &reader,
                         AstStorage *storage);

  ReturnBody body_;
  Where *where_ = nullptr;

 protected:
  explicit With(int uid) : Clause(uid) {}
  With(int uid, ReturnBody &body, Where *where)
      : Clause(uid), body_(body), where_(where) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::With::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class Delete : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto &expr : expressions_) {
        if (!expr->Accept(visitor)) break;
      }
    }
    return visitor.PostVisit(*this);
  }

  Delete *Clone(AstStorage &storage) const override {
    auto *del = storage.Create<Delete>();
    for (auto *expression : expressions_) {
      del->expressions_.push_back(expression->Clone(storage));
    }
    del->detach_ = detach_;
    return del;
  }

  using Clause::Save;
  static Delete *Construct(const capnp::Delete::Reader &reader,
                           AstStorage *storage);

  std::vector<Expression *> expressions_;

  bool detach_ = false;

 protected:
  explicit Delete(int uid) : Clause(uid) {}
  Delete(int uid, bool detach, std::vector<Expression *> expressions)
      : Clause(uid), expressions_(expressions), detach_(detach) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Delete::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class SetProperty : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      property_lookup_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SetProperty *Clone(AstStorage &storage) const override {
    return storage.Create<SetProperty>(property_lookup_->Clone(storage),
                                       expression_->Clone(storage));
  }

  using Clause::Save;
  static SetProperty *Construct(const capnp::SetProperty::Reader &reader,
                                AstStorage *storage);

  PropertyLookup *property_lookup_ = nullptr;
  Expression *expression_ = nullptr;

 protected:
  explicit SetProperty(int uid) : Clause(uid) {}
  SetProperty(int uid, PropertyLookup *property_lookup, Expression *expression)
      : Clause(uid),
        property_lookup_(property_lookup),
        expression_(expression) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::SetProperty::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class SetProperties : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SetProperties *Clone(AstStorage &storage) const override {
    return storage.Create<SetProperties>(identifier_->Clone(storage),
                                         expression_->Clone(storage), update_);
  }

  using Clause::Save;
  static SetProperties *Construct(const capnp::SetProperties::Reader &reader,
                                  AstStorage *storage);

  Identifier *identifier_ = nullptr;
  Expression *expression_ = nullptr;
  bool update_ = false;

 protected:
  explicit SetProperties(int uid) : Clause(uid) {}
  SetProperties(int uid, Identifier *identifier, Expression *expression,
                bool update = false)
      : Clause(uid),
        identifier_(identifier),
        expression_(expression),
        update_(update) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::SetProperties::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class SetLabels : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SetLabels *Clone(AstStorage &storage) const override {
    return storage.Create<SetLabels>(identifier_->Clone(storage), labels_);
  }

  using Clause::Save;
  static SetLabels *Construct(const capnp::SetLabels::Reader &reader,
                              AstStorage *storage);

  Identifier *identifier_ = nullptr;
  std::vector<storage::Label> labels_;

 protected:
  explicit SetLabels(int uid) : Clause(uid) {}
  SetLabels(int uid, Identifier *identifier,
            const std::vector<storage::Label> &labels)
      : Clause(uid), identifier_(identifier), labels_(labels) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::SetLabels::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class RemoveProperty : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      property_lookup_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  RemoveProperty *Clone(AstStorage &storage) const override {
    return storage.Create<RemoveProperty>(property_lookup_->Clone(storage));
  }

  using Clause::Save;
  static RemoveProperty *Construct(const capnp::RemoveProperty::Reader &reader,
                                   AstStorage *storage);

  PropertyLookup *property_lookup_ = nullptr;

 protected:
  explicit RemoveProperty(int uid) : Clause(uid) {}
  RemoveProperty(int uid, PropertyLookup *property_lookup)
      : Clause(uid), property_lookup_(property_lookup) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::RemoveProperty::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class RemoveLabels : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  RemoveLabels *Clone(AstStorage &storage) const override {
    return storage.Create<RemoveLabels>(identifier_->Clone(storage), labels_);
  }

  using Clause::Save;
  static RemoveLabels *Construct(const capnp::RemoveLabels::Reader &reader,
                                 AstStorage *storage);

  Identifier *identifier_ = nullptr;
  std::vector<storage::Label> labels_;

 protected:
  explicit RemoveLabels(int uid) : Clause(uid) {}
  RemoveLabels(int uid, Identifier *identifier,
               const std::vector<storage::Label> &labels)
      : Clause(uid), identifier_(identifier), labels_(labels) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::RemoveLabels::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class Merge : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      bool cont = pattern_->Accept(visitor);
      if (cont) {
        for (auto &set : on_match_) {
          if (!set->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
      if (cont) {
        for (auto &set : on_create_) {
          if (!set->Accept(visitor)) {
            cont = false;
            break;
          }
        }
      }
    }
    return visitor.PostVisit(*this);
  }

  Merge *Clone(AstStorage &storage) const override {
    auto *merge = storage.Create<Merge>();
    merge->pattern_ = pattern_->Clone(storage);
    for (auto *on_match : on_match_) {
      merge->on_match_.push_back(on_match->Clone(storage));
    }
    for (auto *on_create : on_create_) {
      merge->on_create_.push_back(on_create->Clone(storage));
    }
    return merge;
  }

  using Clause::Save;
  static Merge *Construct(const capnp::Merge::Reader &reader,
                          AstStorage *storage);

  Pattern *pattern_ = nullptr;
  std::vector<Clause *> on_match_;
  std::vector<Clause *> on_create_;

 protected:
  explicit Merge(int uid) : Clause(uid) {}
  Merge(int uid, Pattern *pattern, std::vector<Clause *> on_match,
        std::vector<Clause *> on_create)
      : Clause(uid),
        pattern_(pattern),
        on_match_(on_match),
        on_create_(on_create) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Merge::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class Unwind : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      named_expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Unwind *Clone(AstStorage &storage) const override {
    return storage.Create<Unwind>(named_expression_->Clone(storage));
  }

  using Clause::Save;
  static Unwind *Construct(const capnp::Unwind::Reader &reader,
                           AstStorage *storage);

  NamedExpression *named_expression_ = nullptr;

 protected:
  explicit Unwind(int uid) : Clause(uid) {}

  Unwind(int uid, NamedExpression *named_expression)
      : Clause(uid), named_expression_(named_expression) {
    DCHECK(named_expression)
        << "Unwind cannot take nullptr for named_expression";
  }

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::Unwind::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class CreateIndex : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  CreateIndex *Clone(AstStorage &storage) const override {
    return storage.Create<CreateIndex>(label_, property_);
  }

  static CreateIndex *Construct(const capnp::CreateIndex::Reader &reader,
                                AstStorage *storage);
  using Clause::Save;

  storage::Label label_;
  storage::Property property_;

 protected:
  explicit CreateIndex(int uid) : Clause(uid) {}
  CreateIndex(int uid, storage::Label label, storage::Property property)
      : Clause(uid), label_(label), property_(property) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::CreateIndex::Builder *builder,
                    std::vector<int> *saved_uids);
};

class AuthQuery : public Clause {
  friend class AstStorage;

 public:
  enum class Action {
    CREATE_ROLE,
    DROP_ROLE,
    SHOW_ROLES,
    CREATE_USER,
    SET_PASSWORD,
    DROP_USER,
    SHOW_USERS,
    GRANT_ROLE,
    REVOKE_ROLE,
    GRANT_PRIVILEGE,
    DENY_PRIVILEGE,
    REVOKE_PRIVILEGE,
    SHOW_GRANTS,
    SHOW_ROLE_FOR_USER,
    SHOW_USERS_FOR_ROLE
  };

  enum class Privilege { CREATE, DELETE, MATCH, MERGE, SET, AUTH, STREAM };

  Action action_;
  std::string user_;
  std::string role_;
  std::string user_or_role_;
  Expression *password_{nullptr};
  std::vector<Privilege> privileges_;

  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  AuthQuery *Clone(AstStorage &storage) const override {
    return storage.Create<AuthQuery>(
        action_, user_, role_, user_or_role_,
        password_ ? password_->Clone(storage) : nullptr, privileges_);
  }

  static AuthQuery *Construct(const capnp::AuthQuery::Reader &reader,
                              AstStorage *storage);
  using Clause::Save;

 protected:
  explicit AuthQuery(int uid) : Clause(uid) {}

  explicit AuthQuery(int uid, Action action, std::string user, std::string role,
                     std::string user_or_role, Expression *password,
                     std::vector<Privilege> privileges)
      : Clause(uid),
        action_(action),
        user_(user),
        role_(role),
        user_or_role_(user_or_role),
        password_(password),
        privileges_(privileges) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::AuthQuery::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class CreateStream : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  CreateStream *Clone(AstStorage &storage) const override {
    return storage.Create<CreateStream>(
        stream_name_, stream_uri_->Clone(storage),
        stream_topic_->Clone(storage), transform_uri_->Clone(storage),
        batch_interval_in_ms_ ? batch_interval_in_ms_->Clone(storage) : nullptr,
        batch_size_ ? batch_size_->Clone(storage) : nullptr);
  }

  static CreateStream *Construct(const capnp::CreateStream::Reader &reader,
                                 AstStorage *storage);
  using Clause::Save;

  std::string stream_name_;
  Expression *stream_uri_;
  Expression *stream_topic_;
  Expression *transform_uri_;
  Expression *batch_interval_in_ms_;
  Expression *batch_size_;

 protected:
  explicit CreateStream(int uid) : Clause(uid) {}
  CreateStream(int uid, std::string stream_name, Expression *stream_uri,
               Expression *stream_topic, Expression *transform_uri,
               Expression *batch_interval_in_ms, Expression *batch_size)
      : Clause(uid),
        stream_name_(std::move(stream_name)),
        stream_uri_(stream_uri),
        stream_topic_(stream_topic),
        transform_uri_(transform_uri),
        batch_interval_in_ms_(batch_interval_in_ms),
        batch_size_(batch_size) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::CreateStream::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class DropStream : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  DropStream *Clone(AstStorage &storage) const override {
    return storage.Create<DropStream>(stream_name_);
  }

  static DropStream *Construct(const capnp::DropStream::Reader &reader,
                               AstStorage *storage);
  using Clause::Save;

  std::string stream_name_;

 protected:
  explicit DropStream(int uid) : Clause(uid) {}
  DropStream(int uid, std::string stream_name)
      : Clause(uid), stream_name_(std::move(stream_name)) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::DropStream::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class ShowStreams : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  ShowStreams *Clone(AstStorage &storage) const override {
    return storage.Create<ShowStreams>();
  }

  static ShowStreams *Construct(const capnp::ShowStreams::Reader &reader,
                                AstStorage *storage);
  using Clause::Save;

 protected:
  explicit ShowStreams(int uid) : Clause(uid) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::ShowStreams::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class StartStopStream : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  StartStopStream *Clone(AstStorage &storage) const override {
    return storage.Create<StartStopStream>(
        stream_name_, is_start_,
        limit_batches_ ? limit_batches_->Clone(storage) : nullptr);
  }

  static StartStopStream *Construct(
      const capnp::StartStopStream::Reader &reader, AstStorage *storage);
  using Clause::Save;

  std::string stream_name_;
  bool is_start_;
  Expression *limit_batches_;

 protected:
  explicit StartStopStream(int uid) : Clause(uid) {}
  StartStopStream(int uid, std::string stream_name, bool is_start,
                  Expression *limit_batches)
      : Clause(uid),
        stream_name_(std::move(stream_name)),
        is_start_(is_start),
        limit_batches_(limit_batches) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::StartStopStream::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class StartStopAllStreams : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  StartStopAllStreams *Clone(AstStorage &storage) const override {
    return storage.Create<StartStopAllStreams>(is_start_);
  }

  static StartStopAllStreams *Construct(
      const capnp::StartStopAllStreams::Reader &reader, AstStorage *storage);
  using Clause::Save;

  bool is_start_;

 protected:
  explicit StartStopAllStreams(int uid) : Clause(uid) {}
  StartStopAllStreams(int uid, bool is_start)
      : Clause(uid), is_start_(is_start) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::StartStopAllStreams::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

class TestStream : public Clause {
  friend class AstStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  TestStream *Clone(AstStorage &storage) const override {
    return storage.Create<TestStream>(
        stream_name_,
        limit_batches_ ? limit_batches_->Clone(storage) : nullptr);
  }

  static TestStream *Construct(const capnp::TestStream::Reader &reader,
                               AstStorage *storage);
  using Clause::Save;

  std::string stream_name_;
  Expression *limit_batches_;

 protected:
  explicit TestStream(int uid) : Clause(uid) {}
  TestStream(int uid, std::string stream_name, Expression *limit_batches)
      : Clause(uid),
        stream_name_(std::move(stream_name)),
        limit_batches_(limit_batches) {}

  void Save(capnp::Clause::Builder *builder,
            std::vector<int> *saved_uids) override;
  virtual void Save(capnp::TestStream::Builder *builder,
                    std::vector<int> *saved_uids);
  void Load(const capnp::Tree::Reader &base_reader, AstStorage *storage,
            std::vector<int> *loaded_uids) override;
};

#undef CLONE_BINARY_EXPRESSION
#undef CLONE_UNARY_EXPRESSION

}  // namespace query
