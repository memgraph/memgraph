#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "boost/serialization/base_object.hpp"
#include "boost/serialization/export.hpp"
#include "boost/serialization/split_member.hpp"
#include "boost/serialization/string.hpp"
#include "boost/serialization/vector.hpp"

#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/typed_value.hpp"
#include "storage/types.hpp"
#include "utils/serialization.hpp"

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
  auto Clone(AstTreeStorage &storage) const->std::remove_const<              \
      std::remove_pointer<decltype(this)>::type>::type * override {          \
    return storage.Create<                                                   \
        std::remove_cv<std::remove_reference<decltype(*this)>::type>::type>( \
        expression1_->Clone(storage), expression2_->Clone(storage));         \
  }
#define CLONE_UNARY_EXPRESSION                                               \
  auto Clone(AstTreeStorage &storage) const->std::remove_const<              \
      std::remove_pointer<decltype(this)>::type>::type * override {          \
    return storage.Create<                                                   \
        std::remove_cv<std::remove_reference<decltype(*this)>::type>::type>( \
        expression_->Clone(storage));                                        \
  }

#define SERIALIZE_USING_BASE(BaseClass)                      \
  template <class TArchive>                                  \
  void serialize(TArchive &ar, const unsigned int) {         \
    ar &boost::serialization::base_object<BaseClass>(*this); \
  }

class Tree;


// It would be better to call this AstTree, but we already have a class Tree,
// which could be renamed to Node or AstTreeNode, but we also have a class
// called NodeAtom...
class AstTreeStorage {
 public:
  AstTreeStorage();
  AstTreeStorage(const AstTreeStorage &) = delete;
  AstTreeStorage &operator=(const AstTreeStorage &) = delete;
  AstTreeStorage(AstTreeStorage &&) = default;
  AstTreeStorage &operator=(AstTreeStorage &&) = default;

  template <typename T, typename... Args>
  T *Create(Args &&... args) {
    // Never call create for a Query. Call query() instead.
    static_assert(!std::is_same<T, Query>::value, "Call query() instead");
    T *p = new T(next_uid_++, std::forward<Args>(args)...);
    storage_.emplace_back(p);
    return p;
  }

  Query *query() const;

  /// Id for using get_helper<AstTreeStorage> in boost archives.
  static void * const kHelperId;

  /// Load an Ast Node into this storage.
  template <class TArchive, class TNode>
  void Load(TArchive &ar, TNode &node) {
    auto &tmp_ast = ar.template get_helper<AstTreeStorage>(kHelperId);
    std::swap(*this, tmp_ast);
    ar >> node;
    std::swap(*this, tmp_ast);
  }

  /// Load a Query into this storage.
  template <class TArchive>
  void Load(TArchive &ar) {
    Load(ar, *query());
  }

 private:
  int next_uid_ = 0;
  std::vector<std::unique_ptr<Tree>> storage_;

  template <class TArchive, class TNode>
  friend void LoadPointer(TArchive &ar, TNode *&node);
};

template <class TArchive, class TNode>
void SavePointer(TArchive &ar, TNode *node) {
  ar << node;
}

template <class TArchive, class TNode>
void LoadPointer(TArchive &ar, TNode *&node) {
  ar >> node;
  if (node) {
    auto &ast_storage =
        ar.template get_helper<AstTreeStorage>(AstTreeStorage::kHelperId);
    auto found =
        std::find_if(ast_storage.storage_.begin(), ast_storage.storage_.end(),
                     [&](const auto &n) { return n->uid() == node->uid(); });
    // Boost makes sure pointers to same address are deserialized only once, so
    // we only need to add nodes to the storage only on the first load.
    DCHECK(ast_storage.storage_.end() == found ||
           dynamic_cast<TNode *>(found->get()) == node);
    if (ast_storage.storage_.end() == found) {
      ast_storage.storage_.emplace_back(node);
      ast_storage.next_uid_ = std::max(ast_storage.next_uid_, node->uid() + 1);
    }
  }
}

template <class TArchive, class TNode>
void SavePointers(TArchive &ar, const std::vector<TNode *> &nodes) {
  ar << nodes.size();
  for (auto *node : nodes) {
    SavePointer(ar, node);
  }
}

template <class TArchive, class TNode>
void LoadPointers(TArchive &ar, std::vector<TNode *> &nodes) {
  size_t size = 0;
  ar >> size;
  for (size_t i = 0; i < size; ++i) {
    TNode *node = nullptr;
    LoadPointer(ar, node);
    DCHECK(node) << "Unexpected nullptr serialized";
    nodes.emplace_back(node);
  }
}

class Tree : public ::utils::Visitable<HierarchicalTreeVisitor>,
             ::utils::Visitable<TreeVisitor<TypedValue>> {
  friend class AstTreeStorage;

 public:
  using ::utils::Visitable<HierarchicalTreeVisitor>::Accept;
  using ::utils::Visitable<TreeVisitor<TypedValue>>::Accept;

  int uid() const { return uid_; }

  virtual Tree *Clone(AstTreeStorage &storage) const = 0;

 protected:
  explicit Tree(int uid) : uid_(uid) {}

 private:
  int uid_;

  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &uid_;
  }
};

// Expressions

class Expression : public Tree {
  friend class AstTreeStorage;

 public:
  Expression *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  explicit Expression(int uid) : Tree(uid) {}

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(Tree);
};

class Where : public Tree {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Where *Clone(AstTreeStorage &storage) const override {
    return storage.Create<Where>(expression_->Clone(storage));
  }

  Expression *expression_ = nullptr;

 protected:
  explicit Where(int uid) : Tree(uid) {}
  Where(int uid, Expression *expression) : Tree(uid), expression_(expression) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Tree>(*this);
    SavePointer(ar, expression_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Tree>(*this);
    LoadPointer(ar, expression_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Where *,
                                                        const unsigned int);
};

class BinaryOperator : public Expression {
  friend class AstTreeStorage;

 public:
  Expression *expression1_ = nullptr;
  Expression *expression2_ = nullptr;

  BinaryOperator *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  explicit BinaryOperator(int uid) : Expression(uid) {}
  BinaryOperator(int uid, Expression *expression1, Expression *expression2)
      : Expression(uid), expression1_(expression1), expression2_(expression2) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Expression>(*this);
    SavePointer(ar, expression1_);
    SavePointer(ar, expression2_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Expression>(*this);
    LoadPointer(ar, expression1_);
    LoadPointer(ar, expression2_);
  }
};

class UnaryOperator : public Expression {
  friend class AstTreeStorage;

 public:
  Expression *expression_ = nullptr;

  UnaryOperator *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  explicit UnaryOperator(int uid) : Expression(uid) {}
  UnaryOperator(int uid, Expression *expression)
      : Expression(uid), expression_(expression) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Expression>(*this);
    SavePointer(ar, expression_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Expression>(*this);
    LoadPointer(ar, expression_);
  }
};

class OrOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        OrOperator *,
                                                        const unsigned int);
};

class XorOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        XorOperator *,
                                                        const unsigned int);
};

class AndOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        AndOperator *,
                                                        const unsigned int);
};

class AdditionOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        AdditionOperator *,
                                                        const unsigned int);
};

class SubtractionOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        SubtractionOperator *,
                                                        const unsigned int);
};

class MultiplicationOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(
      TArchive &, MultiplicationOperator *, const unsigned int);
};

class DivisionOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        DivisionOperator *,
                                                        const unsigned int);
};

class ModOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        ModOperator *,
                                                        const unsigned int);
};

class NotEqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        NotEqualOperator *,
                                                        const unsigned int);
};

class EqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        EqualOperator *,
                                                        const unsigned int);
};

class LessOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        LessOperator *,
                                                        const unsigned int);
};

class GreaterOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        GreaterOperator *,
                                                        const unsigned int);
};

class LessEqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        LessEqualOperator *,
                                                        const unsigned int);
};

class GreaterEqualOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        GreaterEqualOperator *,
                                                        const unsigned int);
};

class InListOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        InListOperator *,
                                                        const unsigned int);
};

class ListMapIndexingOperator : public BinaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression1_->Accept(visitor) && expression2_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_BINARY_EXPRESSION;

 protected:
  using BinaryOperator::BinaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(BinaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(
      TArchive &, ListMapIndexingOperator *, const unsigned int);
};

class ListSlicingOperator : public Expression {
  friend class AstTreeStorage;

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

  ListSlicingOperator *Clone(AstTreeStorage &storage) const override {
    return storage.Create<ListSlicingOperator>(
        list_->Clone(storage),
        lower_bound_ ? lower_bound_->Clone(storage) : nullptr,
        upper_bound_ ? upper_bound_->Clone(storage) : nullptr);
  }

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

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Expression>(*this);
    SavePointer(ar, list_);
    SavePointer(ar, lower_bound_);
    SavePointer(ar, upper_bound_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Expression>(*this);
    LoadPointer(ar, list_);
    LoadPointer(ar, lower_bound_);
    LoadPointer(ar, upper_bound_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        ListSlicingOperator *,
                                                        const unsigned int);
};

class IfOperator : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      condition_->Accept(visitor) && then_expression_->Accept(visitor) &&
          else_expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  IfOperator *Clone(AstTreeStorage &storage) const override {
    return storage.Create<IfOperator>(condition_->Clone(storage),
                                      then_expression_->Clone(storage),
                                      else_expression_->Clone(storage));
  }

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

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Expression>(*this);
    SavePointer(ar, condition_);
    SavePointer(ar, then_expression_);
    SavePointer(ar, else_expression_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Expression>(*this);
    LoadPointer(ar, condition_);
    LoadPointer(ar, then_expression_);
    LoadPointer(ar, else_expression_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        IfOperator *,
                                                        const unsigned int);
};

class NotOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;

 protected:
  using UnaryOperator::UnaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(UnaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        NotOperator *,
                                                        const unsigned int);
};

class UnaryPlusOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;

 protected:
  using UnaryOperator::UnaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(UnaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        UnaryPlusOperator *,
                                                        const unsigned int);
};

class UnaryMinusOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;

 protected:
  using UnaryOperator::UnaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(UnaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        UnaryMinusOperator *,
                                                        const unsigned int);
};

class IsNullOperator : public UnaryOperator {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }
  CLONE_UNARY_EXPRESSION;

 protected:
  using UnaryOperator::UnaryOperator;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(UnaryOperator);
  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        IsNullOperator *,
                                                        const unsigned int);
};

class BaseLiteral : public Expression {
  friend class AstTreeStorage;

 public:
  BaseLiteral *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  explicit BaseLiteral(int uid) : Expression(uid) {}

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(Expression);
};

class PrimitiveLiteral : public BaseLiteral {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  PrimitiveLiteral *Clone(AstTreeStorage &storage) const override {
    return storage.Create<PrimitiveLiteral>(value_, token_position_);
  }

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

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<BaseLiteral>(*this);
    ar << token_position_;
    utils::SaveTypedValue(ar, value_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<BaseLiteral>(*this);
    ar >> token_position_;
    utils::LoadTypedValue(ar, value_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        PrimitiveLiteral *,
                                                        const unsigned int);
};

class ListLiteral : public BaseLiteral {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto expr_ptr : elements_)
        if (!expr_ptr->Accept(visitor)) break;
    }
    return visitor.PostVisit(*this);
  }

  ListLiteral *Clone(AstTreeStorage &storage) const override {
    auto *list = storage.Create<ListLiteral>();
    for (auto *element : elements_) {
      list->elements_.push_back(element->Clone(storage));
    }
    return list;
  }

  std::vector<Expression *> elements_;

 protected:
  explicit ListLiteral(int uid) : BaseLiteral(uid) {}
  ListLiteral(int uid, const std::vector<Expression *> &elements)
      : BaseLiteral(uid), elements_(elements) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<BaseLiteral>(*this);
    SavePointers(ar, elements_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<BaseLiteral>(*this);
    LoadPointers(ar, elements_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        ListLiteral *,
                                                        const unsigned int);
};

class MapLiteral : public BaseLiteral {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      for (auto pair : elements_)
        if (!pair.second->Accept(visitor)) break;
    }
    return visitor.PostVisit(*this);
  }

  MapLiteral *Clone(AstTreeStorage &storage) const override {
    auto *map = storage.Create<MapLiteral>();
    for (auto pair : elements_)
      map->elements_.emplace(pair.first, pair.second->Clone(storage));
    return map;
  }

  // maps (property_name, property) to expressions
  std::unordered_map<std::pair<std::string, storage::Property>, Expression *>
      elements_;

 protected:
  explicit MapLiteral(int uid) : BaseLiteral(uid) {}
  MapLiteral(int uid,
             const std::unordered_map<std::pair<std::string, storage::Property>,
                                      Expression *> &elements)
      : BaseLiteral(uid), elements_(elements) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<BaseLiteral>(*this);
    ar << elements_.size();
    for (const auto &element : elements_) {
      const auto &property = element.first;
      ar << property.first;
      ar << property.second;
      SavePointer(ar, element.second);
    }
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<BaseLiteral>(*this);
    size_t size = 0;
    ar >> size;
    for (size_t i = 0; i < size; ++i) {
      std::pair<std::string, storage::Property> property;
      ar >> property.first;
      ar >> property.second;
      Expression *expression = nullptr;
      LoadPointer(ar, expression);
      DCHECK(expression) << "Unexpected nullptr expression serialized";
      elements_.emplace(property, expression);
    }
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        MapLiteral *,
                                                        const unsigned int);
};

class Identifier : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  Identifier *Clone(AstTreeStorage &storage) const override {
    return storage.Create<Identifier>(name_, user_declared_);
  }

  std::string name_;
  bool user_declared_ = true;

 protected:
  Identifier(int uid, const std::string &name) : Expression(uid), name_(name) {}
  Identifier(int uid, const std::string &name, bool user_declared)
      : Expression(uid), name_(name), user_declared_(user_declared) {}

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Expression>(*this);
    ar &name_;
    ar &user_declared_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        Identifier *,
                                                        const unsigned int);
};

class PropertyLookup : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  PropertyLookup *Clone(AstTreeStorage &storage) const override {
    return storage.Create<PropertyLookup>(expression_->Clone(storage),
                                          property_name_, property_);
  }

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

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Expression>(*this);
    SavePointer(ar, expression_);
    ar << property_name_;
    ar << property_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Expression>(*this);
    LoadPointer(ar, expression_);
    ar >> property_name_;
    ar >> property_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        PropertyLookup *,
                                                        const unsigned int);
};

class LabelsTest : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  LabelsTest *Clone(AstTreeStorage &storage) const override {
    return storage.Create<LabelsTest>(expression_->Clone(storage), labels_);
  }

  Expression *expression_ = nullptr;
  std::vector<storage::Label> labels_;

 protected:
  LabelsTest(int uid, Expression *expression,
             const std::vector<storage::Label> &labels)
      : Expression(uid), expression_(expression), labels_(labels) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Expression>(*this);
    SavePointer(ar, expression_);
    ar << labels_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Expression>(*this);
    LoadPointer(ar, expression_);
    ar >> labels_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        LabelsTest *,
                                                        const unsigned int);
};

class Function : public Expression {
  friend class AstTreeStorage;

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

  Function *Clone(AstTreeStorage &storage) const override {
    std::vector<Expression *> arguments;
    for (auto *argument : arguments_) {
      arguments.push_back(argument->Clone(storage));
    }
    return storage.Create<Function>(function_name_, arguments);
  }

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

 private:
  std::string function_name_;
  std::function<TypedValue(const std::vector<TypedValue> &,
                           database::GraphDbAccessor &)>
      function_;

  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Expression>(*this);
    ar << function_name_;
    SavePointers(ar, arguments_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Expression>(*this);
    ar >> function_name_;
    function_ = NameToFunction(function_name_);
    DCHECK(function_) << "Unexpected missing function: " << function_name_;
    LoadPointers(ar, arguments_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Function *,
                                                        const unsigned int);
};

class Aggregation : public BinaryOperator {
  friend class AstTreeStorage;

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

  Aggregation *Clone(AstTreeStorage &storage) const override {
    return storage.Create<Aggregation>(
        expression1_ ? expression1_->Clone(storage) : nullptr,
        expression2_ ? expression2_->Clone(storage) : nullptr, op_);
  }

  Op op_;

 protected:
  /** Aggregation's first expression is the value being aggregated. The second
   * expression is the key used only in COLLECT_MAP. */
  Aggregation(int uid, Expression *expression1, Expression *expression2, Op op)
      : BinaryOperator(uid, expression1, expression2), op_(op) {
    // COUNT without expression denotes COUNT(*) in cypher.
    DCHECK(expression1 || op == Aggregation::Op::COUNT)
        << "All aggregations, except COUNT require expression";
    DCHECK((expression2 == nullptr) ^ (op == Aggregation::Op::COLLECT_MAP))
        << "The second expression is obligatory in COLLECT_MAP and "
           "invalid otherwise";
  }

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<BinaryOperator>(*this);
    ar &op_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        Aggregation *,
                                                        const unsigned int);
};

class All : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && list_expression_->Accept(visitor) &&
          where_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  All *Clone(AstTreeStorage &storage) const override {
    return storage.Create<All>(identifier_->Clone(storage),
                               list_expression_->Clone(storage),
                               where_->Clone(storage));
  }

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

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Expression>(*this);
    SavePointer(ar, identifier_);
    SavePointer(ar, list_expression_);
    SavePointer(ar, where_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Expression>(*this);
    LoadPointer(ar, identifier_);
    LoadPointer(ar, list_expression_);
    LoadPointer(ar, where_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, All *,
                                                        const unsigned int);
};

class ParameterLookup : public Expression {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  ParameterLookup *Clone(AstTreeStorage &storage) const override {
    return storage.Create<ParameterLookup>(token_position_);
  }

  // This field contains token position of *literal* used to create
  // ParameterLookup object. If ParameterLookup object is not created from
  // a literal leave this value at -1.
  int token_position_ = -1;

 protected:
  explicit ParameterLookup(int uid) : Expression(uid) {}
  ParameterLookup(int uid, int token_position)
      : Expression(uid), token_position_(token_position) {}

  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Expression>(*this);
    ar &token_position_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        ParameterLookup *,
                                                        const unsigned int);
};

class NamedExpression : public Tree {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  NamedExpression *Clone(AstTreeStorage &storage) const override {
    return storage.Create<NamedExpression>(name_, expression_->Clone(storage),
                                           token_position_);
  }

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

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Tree>(*this);
    ar << name_;
    SavePointer(ar, expression_);
    ar << token_position_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Tree>(*this);
    ar >> name_;
    LoadPointer(ar, expression_);
    ar >> token_position_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        NamedExpression *,
                                                        const unsigned int);
};

// Pattern atoms

class PatternAtom : public Tree {
  friend class AstTreeStorage;

 public:
  Identifier *identifier_ = nullptr;

  PatternAtom *Clone(AstTreeStorage &storage) const override = 0;

 protected:
  explicit PatternAtom(int uid) : Tree(uid) {}
  PatternAtom(int uid, Identifier *identifier)
      : Tree(uid), identifier_(identifier) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Tree>(*this);
    SavePointer(ar, identifier_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Tree>(*this);
    LoadPointer(ar, identifier_);
  }
};

class NodeAtom : public PatternAtom {
  friend class AstTreeStorage;

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

  NodeAtom *Clone(AstTreeStorage &storage) const override {
    auto *node_atom = storage.Create<NodeAtom>(identifier_->Clone(storage));
    node_atom->labels_ = labels_;
    for (auto property : properties_) {
      node_atom->properties_[property.first] = property.second->Clone(storage);
    }
    return node_atom;
  }

  std::vector<storage::Label> labels_;
  // maps (property_name, property) to an expression
  std::unordered_map<std::pair<std::string, storage::Property>, Expression *>
      properties_;

 protected:
  using PatternAtom::PatternAtom;

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<PatternAtom>(*this);
    ar << labels_;
    ar << properties_.size();
    for (const auto &property : properties_) {
      const auto &key = property.first;
      ar << key.first;
      ar << key.second;
      SavePointer(ar, property.second);
    }
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<PatternAtom>(*this);
    ar >> labels_;
    size_t size = 0;
    ar >> size;
    for (size_t i = 0; i < size; ++i) {
      std::pair<std::string, storage::Property> property;
      ar >> property.first;
      ar >> property.second;
      Expression *expression = nullptr;
      LoadPointer(ar, expression);
      DCHECK(expression) << "Unexpected nullptr expression serialized";
      properties_.emplace(property, expression);
    }
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, NodeAtom *,
                                                        const unsigned int);
};

class EdgeAtom : public PatternAtom {
  friend class AstTreeStorage;

  template <typename TPtr>
  static TPtr *CloneOpt(TPtr *ptr, AstTreeStorage &storage) {
    return ptr ? ptr->Clone(storage) : nullptr;
  }

 public:
  enum class Type { SINGLE, DEPTH_FIRST, BREADTH_FIRST };
  enum class Direction { IN, OUT, BOTH };

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
        upper_bound_->Accept(visitor);
      }
    }
    return visitor.PostVisit(*this);
  }

  EdgeAtom *Clone(AstTreeStorage &storage) const override {
    auto *edge_atom = storage.Create<EdgeAtom>(identifier_->Clone(storage));
    edge_atom->direction_ = direction_;
    edge_atom->type_ = type_;
    edge_atom->edge_types_ = edge_types_;
    for (auto property : properties_) {
      edge_atom->properties_[property.first] = property.second->Clone(storage);
    }
    edge_atom->lower_bound_ = CloneOpt(lower_bound_, storage);
    edge_atom->upper_bound_ = CloneOpt(upper_bound_, storage);
    edge_atom->inner_edge_ = CloneOpt(inner_edge_, storage);
    edge_atom->inner_node_ = CloneOpt(inner_node_, storage);
    edge_atom->filter_expression_ = CloneOpt(filter_expression_, storage);
    return edge_atom;
  }

  bool IsVariable() const {
    switch (type_) {
      case Type::DEPTH_FIRST:
      case Type::BREADTH_FIRST:
        return true;
      default:
        return false;
    }
  }

  Type type_ = Type::SINGLE;
  Direction direction_ = Direction::BOTH;
  std::vector<storage::EdgeType> edge_types_;
  std::unordered_map<std::pair<std::string, storage::Property>, Expression *>
      properties_;

  // Used in variable length and BFS expansions. Bounds can be nullptr. Inner
  // element symbols can only be null in SINGLE expansion. Filter can be
  // nullptr.
  Expression *lower_bound_ = nullptr;
  Expression *upper_bound_ = nullptr;
  Identifier *inner_edge_ = nullptr;
  Identifier *inner_node_ = nullptr;
  Expression *filter_expression_ = nullptr;

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

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<PatternAtom>(*this);
    ar << type_;
    ar << direction_;
    ar << edge_types_;
    ar << properties_.size();
    for (const auto &property : properties_) {
      const auto &key = property.first;
      ar << key.first;
      ar << key.second;
      SavePointer(ar, property.second);
    }
    SavePointer(ar, lower_bound_);
    SavePointer(ar, upper_bound_);
    SavePointer(ar, inner_edge_);
    SavePointer(ar, inner_node_);
    SavePointer(ar, filter_expression_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<PatternAtom>(*this);
    ar >> type_;
    ar >> direction_;
    ar >> edge_types_;
    size_t size = 0;
    ar >> size;
    for (size_t i = 0; i < size; ++i) {
      std::pair<std::string, storage::Property> property;
      ar >> property.first;
      ar >> property.second;
      Expression *expression = nullptr;
      LoadPointer(ar, expression);
      DCHECK(expression) << "Unexpected nullptr expression serialized";
      properties_.emplace(property, expression);
    }
    LoadPointer(ar, lower_bound_);
    LoadPointer(ar, upper_bound_);
    LoadPointer(ar, inner_edge_);
    LoadPointer(ar, inner_node_);
    LoadPointer(ar, filter_expression_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, EdgeAtom *,
                                                        const unsigned int);
};

class Pattern : public Tree {
  friend class AstTreeStorage;

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

  Pattern *Clone(AstTreeStorage &storage) const override {
    auto *pattern = storage.Create<Pattern>();
    pattern->identifier_ = identifier_->Clone(storage);
    for (auto *atom : atoms_) {
      pattern->atoms_.push_back(atom->Clone(storage));
    }
    return pattern;
  }

  Identifier *identifier_ = nullptr;
  std::vector<PatternAtom *> atoms_;

 protected:
  explicit Pattern(int uid) : Tree(uid) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Tree>(*this);
    SavePointer(ar, identifier_);
    SavePointers(ar, atoms_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Tree>(*this);
    LoadPointer(ar, identifier_);
    LoadPointers(ar, atoms_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Pattern *,
                                                        const unsigned int);
};

// Clause

class Clause : public Tree {
  friend class AstTreeStorage;

 public:
  explicit Clause(int uid) : Tree(uid) {}

  Clause *Clone(AstTreeStorage &storage) const override = 0;

 private:
  friend class boost::serialization::access;
  SERIALIZE_USING_BASE(Tree);
};

// SingleQuery

class SingleQuery : public Tree {
  friend class AstTreeStorage;

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

  SingleQuery *Clone(AstTreeStorage &storage) const override {
    auto *single_query = storage.Create<SingleQuery>();
    for (auto *clause : clauses_) {
      single_query->clauses_.push_back(clause->Clone(storage));
    }
    return single_query;
  }

  std::vector<Clause *> clauses_;

 protected:
  explicit SingleQuery(int uid) : Tree(uid) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Tree>(*this);
    SavePointers(ar, clauses_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Tree>(*this);
    LoadPointers(ar, clauses_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        SingleQuery *,
                                                        const unsigned int);
};

// CypherUnion

class CypherUnion : public Tree {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      single_query_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  CypherUnion *Clone(AstTreeStorage &storage) const override {
    auto cypher_union = storage.Create<CypherUnion>(distinct_);
    cypher_union->single_query_ = single_query_->Clone(storage);
    cypher_union->union_symbols_ = union_symbols_;
    return cypher_union;
  }

  SingleQuery *single_query_ = nullptr;
  bool distinct_ = false;
  /**
   * @brief Holds symbols that are created during symbol generation phase.
   * These symbols are used when UNION/UNION ALL combines single query results.
   */
  std::vector<Symbol> union_symbols_;

 protected:
  explicit CypherUnion(int uid) : Tree(uid) {}
  CypherUnion(int uid, bool distinct) : Tree(uid), distinct_(distinct) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Tree>(*this);
    SavePointer(ar, single_query_);
    ar << distinct_;
    ar << union_symbols_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Tree>(*this);
    LoadPointer(ar, single_query_);
    ar >> distinct_;
    ar >> union_symbols_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        CypherUnion *,
                                                        const unsigned int);
};

// Queries

class Query : public Tree {
  friend class AstTreeStorage;

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
  Query *Clone(AstTreeStorage &storage) const override {
    auto *query = storage.query();
    query->single_query_ = single_query_->Clone(storage);
    for (auto *cypher_union : cypher_unions_) {
      query->cypher_unions_.push_back(cypher_union->Clone(storage));
    }
    return query;
  }

  SingleQuery *single_query_ = nullptr;
  std::vector<CypherUnion *> cypher_unions_;

 protected:
  explicit Query(int uid) : Tree(uid) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Tree>(*this);
    SavePointer(ar, single_query_);
    SavePointers(ar, cypher_unions_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Tree>(*this);
    LoadPointer(ar, single_query_);
    LoadPointers(ar, cypher_unions_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Query *,
                                                        const unsigned int);
};

// Clauses

class Create : public Clause {
  friend class AstTreeStorage;

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

  Create *Clone(AstTreeStorage &storage) const override {
    auto *create = storage.Create<Create>();
    for (auto *pattern : patterns_) {
      create->patterns_.push_back(pattern->Clone(storage));
    }
    return create;
  }

  std::vector<Pattern *> patterns_;

 protected:
  explicit Create(int uid) : Clause(uid) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointers(ar, patterns_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointers(ar, patterns_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Create *,
                                                        const unsigned int);
};

class Match : public Clause {
  friend class AstTreeStorage;

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

  Match *Clone(AstTreeStorage &storage) const override {
    auto *match = storage.Create<Match>(optional_);
    for (auto *pattern : patterns_) {
      match->patterns_.push_back(pattern->Clone(storage));
    }
    match->where_ = where_ ? where_->Clone(storage) : nullptr;
    return match;
  }

  std::vector<Pattern *> patterns_;
  Where *where_ = nullptr;
  bool optional_ = false;

 protected:
  explicit Match(int uid) : Clause(uid) {}
  Match(int uid, bool optional) : Clause(uid), optional_(optional) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointers(ar, patterns_);
    SavePointer(ar, where_);
    ar << optional_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointers(ar, patterns_);
    LoadPointer(ar, where_);
    ar >> optional_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Match *,
                                                        const unsigned int);
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

// Deep copy ReturnBody.
// TODO: Think about turning ReturnBody to class and making this
// function class member.
ReturnBody CloneReturnBody(AstTreeStorage &storage, const ReturnBody &body);

template <class TArchive>
void serialize(TArchive &ar, ReturnBody &body,
               const unsigned int file_version) {
  boost::serialization::split_free(ar, body, file_version);
}

template <class TArchive>
void save(TArchive &ar, const ReturnBody &body, const unsigned int) {
  ar << body.distinct;
  ar << body.all_identifiers;
  SavePointers(ar, body.named_expressions);
  ar << body.order_by.size();
  for (const auto &order_by : body.order_by) {
    ar << order_by.first;
    SavePointer(ar, order_by.second);
  }
  SavePointer(ar, body.skip);
  SavePointer(ar, body.limit);
}

template <class TArchive>
void load(TArchive &ar, ReturnBody &body, const unsigned int) {
  ar >> body.distinct;
  ar >> body.all_identifiers;
  LoadPointers(ar, body.named_expressions);
  size_t size = 0;
  ar >> size;
  for (size_t i = 0; i < size; ++i) {
    std::pair<Ordering, Expression *> order_by;
    ar >> order_by.first;
    LoadPointer(ar, order_by.second);
    DCHECK(order_by.second) << "Unexpected nullptr serialized";
    body.order_by.emplace_back(order_by);
  }
  LoadPointer(ar, body.skip);
  LoadPointer(ar, body.limit);
}

class Return : public Clause {
  friend class AstTreeStorage;

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

  Return *Clone(AstTreeStorage &storage) const override {
    auto *ret = storage.Create<Return>();
    ret->body_ = CloneReturnBody(storage, body_);
    return ret;
  }

  ReturnBody body_;

 protected:
  explicit Return(int uid) : Clause(uid) {}

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Clause>(*this);
    ar &body_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Return *,
                                                        const unsigned int);
};

class With : public Clause {
  friend class AstTreeStorage;

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

  With *Clone(AstTreeStorage &storage) const override {
    auto *with = storage.Create<With>();
    with->body_ = CloneReturnBody(storage, body_);
    with->where_ = where_ ? where_->Clone(storage) : nullptr;
    return with;
  }

  ReturnBody body_;
  Where *where_ = nullptr;

 protected:
  explicit With(int uid) : Clause(uid) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    ar << body_;
    SavePointer(ar, where_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    ar >> body_;
    LoadPointer(ar, where_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, With *,
                                                        const unsigned int);
};

class Delete : public Clause {
  friend class AstTreeStorage;

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

  Delete *Clone(AstTreeStorage &storage) const override {
    auto *del = storage.Create<Delete>();
    for (auto *expression : expressions_) {
      del->expressions_.push_back(expression->Clone(storage));
    }
    del->detach_ = detach_;
    return del;
  }

  std::vector<Expression *> expressions_;
  bool detach_ = false;

 protected:
  explicit Delete(int uid) : Clause(uid) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointers(ar, expressions_);
    ar << detach_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointers(ar, expressions_);
    ar >> detach_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Delete *,
                                                        const unsigned int);
};

class SetProperty : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      property_lookup_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SetProperty *Clone(AstTreeStorage &storage) const override {
    return storage.Create<SetProperty>(property_lookup_->Clone(storage),
                                       expression_->Clone(storage));
  }

  PropertyLookup *property_lookup_ = nullptr;
  Expression *expression_ = nullptr;

 protected:
  explicit SetProperty(int uid) : Clause(uid) {}
  SetProperty(int uid, PropertyLookup *property_lookup, Expression *expression)
      : Clause(uid),
        property_lookup_(property_lookup),
        expression_(expression) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointer(ar, property_lookup_);
    SavePointer(ar, expression_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointer(ar, property_lookup_);
    LoadPointer(ar, expression_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        SetProperty *,
                                                        const unsigned int);
};

class SetProperties : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor) && expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SetProperties *Clone(AstTreeStorage &storage) const override {
    return storage.Create<SetProperties>(identifier_->Clone(storage),
                                         expression_->Clone(storage), update_);
  }

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

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointer(ar, identifier_);
    SavePointer(ar, expression_);
    ar << update_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointer(ar, identifier_);
    LoadPointer(ar, expression_);
    ar >> update_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        SetProperties *,
                                                        const unsigned int);
};

class SetLabels : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  SetLabels *Clone(AstTreeStorage &storage) const override {
    return storage.Create<SetLabels>(identifier_->Clone(storage), labels_);
  }

  Identifier *identifier_ = nullptr;
  std::vector<storage::Label> labels_;

 protected:
  explicit SetLabels(int uid) : Clause(uid) {}
  SetLabels(int uid, Identifier *identifier,
            const std::vector<storage::Label> &labels)
      : Clause(uid), identifier_(identifier), labels_(labels) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointer(ar, identifier_);
    ar << labels_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointer(ar, identifier_);
    ar >> labels_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, SetLabels *,
                                                        const unsigned int);
};

class RemoveProperty : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      property_lookup_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  RemoveProperty *Clone(AstTreeStorage &storage) const override {
    return storage.Create<RemoveProperty>(property_lookup_->Clone(storage));
  }

  PropertyLookup *property_lookup_ = nullptr;

 protected:
  explicit RemoveProperty(int uid) : Clause(uid) {}
  RemoveProperty(int uid, PropertyLookup *property_lookup)
      : Clause(uid), property_lookup_(property_lookup) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointer(ar, property_lookup_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointer(ar, property_lookup_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        RemoveProperty *,
                                                        const unsigned int);
};

class RemoveLabels : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      identifier_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  RemoveLabels *Clone(AstTreeStorage &storage) const override {
    return storage.Create<RemoveLabels>(identifier_->Clone(storage), labels_);
  }

  Identifier *identifier_ = nullptr;
  std::vector<storage::Label> labels_;

 protected:
  explicit RemoveLabels(int uid) : Clause(uid) {}
  RemoveLabels(int uid, Identifier *identifier,
               const std::vector<storage::Label> &labels)
      : Clause(uid), identifier_(identifier), labels_(labels) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointer(ar, identifier_);
    ar << labels_;
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointer(ar, identifier_);
    ar >> labels_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        RemoveLabels *,
                                                        const unsigned int);
};

class Merge : public Clause {
  friend class AstTreeStorage;

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

  Merge *Clone(AstTreeStorage &storage) const override {
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

  Pattern *pattern_ = nullptr;
  std::vector<Clause *> on_match_;
  std::vector<Clause *> on_create_;

 protected:
  explicit Merge(int uid) : Clause(uid) {}

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointer(ar, pattern_);
    SavePointers(ar, on_match_);
    SavePointers(ar, on_create_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointer(ar, pattern_);
    LoadPointers(ar, on_match_);
    LoadPointers(ar, on_create_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Merge *,
                                                        const unsigned int);
};

class Unwind : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  bool Accept(HierarchicalTreeVisitor &visitor) override {
    if (visitor.PreVisit(*this)) {
      named_expression_->Accept(visitor);
    }
    return visitor.PostVisit(*this);
  }

  Unwind *Clone(AstTreeStorage &storage) const override {
    return storage.Create<Unwind>(named_expression_->Clone(storage));
  }

  NamedExpression *named_expression_ = nullptr;

 protected:
  explicit Unwind(int uid) : Clause(uid) {}

  Unwind(int uid, NamedExpression *named_expression)
      : Clause(uid), named_expression_(named_expression) {
    DCHECK(named_expression)
        << "Unwind cannot take nullptr for named_expression";
  }

 private:
  friend class boost::serialization::access;

  BOOST_SERIALIZATION_SPLIT_MEMBER();

  template <class TArchive>
  void save(TArchive &ar, const unsigned int) const {
    ar << boost::serialization::base_object<Clause>(*this);
    SavePointer(ar, named_expression_);
  }

  template <class TArchive>
  void load(TArchive &ar, const unsigned int) {
    ar >> boost::serialization::base_object<Clause>(*this);
    LoadPointer(ar, named_expression_);
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &, Unwind *,
                                                        const unsigned int);
};

class CreateIndex : public Clause {
  friend class AstTreeStorage;

 public:
  DEFVISITABLE(TreeVisitor<TypedValue>);
  DEFVISITABLE(HierarchicalTreeVisitor);

  CreateIndex *Clone(AstTreeStorage &storage) const override {
    return storage.Create<CreateIndex>(label_, property_);
  }

  storage::Label label_;
  storage::Property property_;

 protected:
  explicit CreateIndex(int uid) : Clause(uid) {}
  CreateIndex(int uid, storage::Label label, storage::Property property)
      : Clause(uid), label_(label), property_(property) {}

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, const unsigned int) {
    ar &boost::serialization::base_object<Clause>(*this);
    ar &label_;
    ar &property_;
  }

  template <class TArchive>
  friend void boost::serialization::load_construct_data(TArchive &,
                                                        CreateIndex *,
                                                        const unsigned int);
};

#undef CLONE_BINARY_EXPRESSION
#undef CLONE_UNARY_EXPRESSION
#undef SERIALIZE_USING_BASE

}  // namespace query

// All of the serialization cruft follows

#define LOAD_AND_CONSTRUCT(DerivedClass, ...)             \
  template <class TArchive>                               \
  void load_construct_data(TArchive &, DerivedClass *cls, \
                           const unsigned int) {          \
    ::new (cls) DerivedClass(__VA_ARGS__);                \
  }

namespace boost::serialization {

LOAD_AND_CONSTRUCT(query::Where, 0);
LOAD_AND_CONSTRUCT(query::OrOperator, 0);
LOAD_AND_CONSTRUCT(query::XorOperator, 0);
LOAD_AND_CONSTRUCT(query::AndOperator, 0);
LOAD_AND_CONSTRUCT(query::AdditionOperator, 0);
LOAD_AND_CONSTRUCT(query::SubtractionOperator, 0);
LOAD_AND_CONSTRUCT(query::MultiplicationOperator, 0);
LOAD_AND_CONSTRUCT(query::DivisionOperator, 0);
LOAD_AND_CONSTRUCT(query::ModOperator, 0);
LOAD_AND_CONSTRUCT(query::NotEqualOperator, 0);
LOAD_AND_CONSTRUCT(query::EqualOperator, 0);
LOAD_AND_CONSTRUCT(query::LessOperator, 0);
LOAD_AND_CONSTRUCT(query::GreaterOperator, 0);
LOAD_AND_CONSTRUCT(query::LessEqualOperator, 0);
LOAD_AND_CONSTRUCT(query::GreaterEqualOperator, 0);
LOAD_AND_CONSTRUCT(query::InListOperator, 0);
LOAD_AND_CONSTRUCT(query::ListMapIndexingOperator, 0);
LOAD_AND_CONSTRUCT(query::ListSlicingOperator, 0, nullptr, nullptr, nullptr);
LOAD_AND_CONSTRUCT(query::IfOperator, 0, nullptr, nullptr, nullptr);
LOAD_AND_CONSTRUCT(query::NotOperator, 0);
LOAD_AND_CONSTRUCT(query::UnaryPlusOperator, 0);
LOAD_AND_CONSTRUCT(query::UnaryMinusOperator, 0);
LOAD_AND_CONSTRUCT(query::IsNullOperator, 0);
LOAD_AND_CONSTRUCT(query::PrimitiveLiteral, 0);
LOAD_AND_CONSTRUCT(query::ListLiteral, 0);
LOAD_AND_CONSTRUCT(query::MapLiteral, 0);
LOAD_AND_CONSTRUCT(query::Identifier, 0, "");
LOAD_AND_CONSTRUCT(query::PropertyLookup, 0, nullptr, "", storage::Property());
LOAD_AND_CONSTRUCT(query::LabelsTest, 0, nullptr,
                   std::vector<storage::Label>());
LOAD_AND_CONSTRUCT(query::Function, 0);
LOAD_AND_CONSTRUCT(query::Aggregation, 0, nullptr, nullptr,
                   query::Aggregation::Op::COUNT);
LOAD_AND_CONSTRUCT(query::All, 0, nullptr, nullptr, nullptr);
LOAD_AND_CONSTRUCT(query::ParameterLookup, 0);
LOAD_AND_CONSTRUCT(query::NamedExpression, 0);
LOAD_AND_CONSTRUCT(query::NodeAtom, 0);
LOAD_AND_CONSTRUCT(query::EdgeAtom, 0);
LOAD_AND_CONSTRUCT(query::Pattern, 0);
LOAD_AND_CONSTRUCT(query::SingleQuery, 0);
LOAD_AND_CONSTRUCT(query::CypherUnion, 0);
LOAD_AND_CONSTRUCT(query::Query, 0);
LOAD_AND_CONSTRUCT(query::Create, 0);
LOAD_AND_CONSTRUCT(query::Match, 0);
LOAD_AND_CONSTRUCT(query::Return, 0);
LOAD_AND_CONSTRUCT(query::With, 0);
LOAD_AND_CONSTRUCT(query::Delete, 0);
LOAD_AND_CONSTRUCT(query::SetProperty, 0);
LOAD_AND_CONSTRUCT(query::SetProperties, 0);
LOAD_AND_CONSTRUCT(query::SetLabels, 0);
LOAD_AND_CONSTRUCT(query::RemoveProperty, 0);
LOAD_AND_CONSTRUCT(query::RemoveLabels, 0);
LOAD_AND_CONSTRUCT(query::Merge, 0);
LOAD_AND_CONSTRUCT(query::Unwind, 0);
LOAD_AND_CONSTRUCT(query::CreateIndex, 0);

}  // namespace boost::serialization

#undef LOAD_AND_CONSTRUCT

BOOST_CLASS_EXPORT_KEY(query::Query);
BOOST_CLASS_EXPORT_KEY(query::SingleQuery);
BOOST_CLASS_EXPORT_KEY(query::CypherUnion);
BOOST_CLASS_EXPORT_KEY(query::NamedExpression);
BOOST_CLASS_EXPORT_KEY(query::OrOperator);
BOOST_CLASS_EXPORT_KEY(query::XorOperator);
BOOST_CLASS_EXPORT_KEY(query::AndOperator);
BOOST_CLASS_EXPORT_KEY(query::NotOperator);
BOOST_CLASS_EXPORT_KEY(query::AdditionOperator);
BOOST_CLASS_EXPORT_KEY(query::SubtractionOperator);
BOOST_CLASS_EXPORT_KEY(query::MultiplicationOperator);
BOOST_CLASS_EXPORT_KEY(query::DivisionOperator);
BOOST_CLASS_EXPORT_KEY(query::ModOperator);
BOOST_CLASS_EXPORT_KEY(query::NotEqualOperator);
BOOST_CLASS_EXPORT_KEY(query::EqualOperator);
BOOST_CLASS_EXPORT_KEY(query::LessOperator);
BOOST_CLASS_EXPORT_KEY(query::GreaterOperator);
BOOST_CLASS_EXPORT_KEY(query::LessEqualOperator);
BOOST_CLASS_EXPORT_KEY(query::GreaterEqualOperator);
BOOST_CLASS_EXPORT_KEY(query::InListOperator);
BOOST_CLASS_EXPORT_KEY(query::ListMapIndexingOperator);
BOOST_CLASS_EXPORT_KEY(query::ListSlicingOperator);
BOOST_CLASS_EXPORT_KEY(query::IfOperator);
BOOST_CLASS_EXPORT_KEY(query::UnaryPlusOperator);
BOOST_CLASS_EXPORT_KEY(query::UnaryMinusOperator);
BOOST_CLASS_EXPORT_KEY(query::IsNullOperator);
BOOST_CLASS_EXPORT_KEY(query::ListLiteral);
BOOST_CLASS_EXPORT_KEY(query::MapLiteral);
BOOST_CLASS_EXPORT_KEY(query::PropertyLookup);
BOOST_CLASS_EXPORT_KEY(query::LabelsTest);
BOOST_CLASS_EXPORT_KEY(query::Aggregation);
BOOST_CLASS_EXPORT_KEY(query::Function);
BOOST_CLASS_EXPORT_KEY(query::All);
BOOST_CLASS_EXPORT_KEY(query::ParameterLookup);
BOOST_CLASS_EXPORT_KEY(query::Create);
BOOST_CLASS_EXPORT_KEY(query::Match);
BOOST_CLASS_EXPORT_KEY(query::Return);
BOOST_CLASS_EXPORT_KEY(query::With);
BOOST_CLASS_EXPORT_KEY(query::Pattern);
BOOST_CLASS_EXPORT_KEY(query::NodeAtom);
BOOST_CLASS_EXPORT_KEY(query::EdgeAtom);
BOOST_CLASS_EXPORT_KEY(query::Delete);
BOOST_CLASS_EXPORT_KEY(query::Where);
BOOST_CLASS_EXPORT_KEY(query::SetProperty);
BOOST_CLASS_EXPORT_KEY(query::SetProperties);
BOOST_CLASS_EXPORT_KEY(query::SetLabels);
BOOST_CLASS_EXPORT_KEY(query::RemoveProperty);
BOOST_CLASS_EXPORT_KEY(query::RemoveLabels);
BOOST_CLASS_EXPORT_KEY(query::Merge);
BOOST_CLASS_EXPORT_KEY(query::Unwind);
BOOST_CLASS_EXPORT_KEY(query::Identifier);
BOOST_CLASS_EXPORT_KEY(query::PrimitiveLiteral);
BOOST_CLASS_EXPORT_KEY(query::CreateIndex);
