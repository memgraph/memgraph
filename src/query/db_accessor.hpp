#pragma once

#include <optional>

#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>

#include "query/exceptions.hpp"
#include "storage/common/types/property_value.hpp"
#include "storage/common/types/types.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/view.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"

#ifdef MG_SINGLE_NODE_V2
#include "storage/v2/storage.hpp"
#else
#include "database/graph_db_accessor.hpp"
#endif

namespace query {

class VertexAccessor;

#ifdef MG_SINGLE_NODE_V2
class EdgeAccessor final {
 public:
  storage::EdgeAccessor impl_;

 public:
  explicit EdgeAccessor(storage::EdgeAccessor impl) : impl_(std::move(impl)) {}

  storage::EdgeType EdgeType() const { return impl_.EdgeType(); }

  auto Properties(storage::View view) const { return impl_.Properties(view); }

  storage::Result<PropertyValue> GetProperty(storage::View view,
                                             storage::Property key) const {
    return impl_.GetProperty(key, view);
  }

  storage::Result<bool> SetProperty(storage::Property key,
                                    const PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> RemoveProperty(storage::Property key) {
    return SetProperty(key, PropertyValue());
  }

  utils::BasicResult<storage::Error, void> ClearProperties() {
    auto ret = impl_.ClearProperties();
    if (ret.HasError()) return ret.GetError();
    return {};
  }

  VertexAccessor To() const;

  VertexAccessor From() const;

  bool IsCycle() const;

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  auto Gid() const { return impl_.Gid(); }

  bool operator==(const EdgeAccessor &e) const { return impl_ == e.impl_; }

  bool operator!=(const EdgeAccessor &e) const { return !(*this == e); }
};

class VertexAccessor final {
 public:
  storage::VertexAccessor impl_;

  static EdgeAccessor MakeEdgeAccessor(const storage::EdgeAccessor impl) {
    return EdgeAccessor(impl);
  }

 public:
  explicit VertexAccessor(storage::VertexAccessor impl)
      : impl_(std::move(impl)) {}

  auto Labels(storage::View view) const { return impl_.Labels(view); }

  storage::Result<bool> AddLabel(storage::Label label) {
    return impl_.AddLabel(label);
  }

  storage::Result<bool> RemoveLabel(storage::Label label) {
    return impl_.RemoveLabel(label);
  }

  storage::Result<bool> HasLabel(storage::View view,
                                 storage::Label label) const {
    return impl_.HasLabel(label, view);
  }

  auto Properties(storage::View view) const { return impl_.Properties(view); }

  storage::Result<PropertyValue> GetProperty(storage::View view,
                                             storage::Property key) const {
    return impl_.GetProperty(key, view);
  }

  storage::Result<bool> SetProperty(storage::Property key,
                                    const PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::Result<bool> RemoveProperty(storage::Property key) {
    return SetProperty(key, PropertyValue());
  }

  utils::BasicResult<storage::Error, void> ClearProperties() {
    auto ret = impl_.ClearProperties();
    if (ret.HasError()) return ret.GetError();
    return {};
  }

  auto InEdges(storage::View view,
               const std::vector<storage::EdgeType> &edge_types) const
      -> storage::Result<decltype(
          iter::imap(MakeEdgeAccessor, *impl_.InEdges(edge_types, view)))> {
    auto maybe_edges = impl_.InEdges(edge_types, view);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  }

  auto InEdges(storage::View view) const { return InEdges(view, {}); }

  auto InEdges(storage::View view,
               const std::vector<storage::EdgeType> &edge_types,
               const VertexAccessor &dest) const
      -> storage::Result<decltype(
          iter::imap(MakeEdgeAccessor, *impl_.InEdges(edge_types, view)))> {
    auto maybe_edges = impl_.InEdges(edge_types, view);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    std::vector<storage::EdgeAccessor> reduced_edges;
    reduced_edges.reserve(maybe_edges->size());
    for (auto &edge : *maybe_edges) {
      if (edge.FromVertex() == dest.impl_) {
        reduced_edges.push_back(edge);
      }
    }
    return iter::imap(MakeEdgeAccessor, std::move(reduced_edges));
  }

  auto OutEdges(storage::View view,
                const std::vector<storage::EdgeType> &edge_types) const
      -> storage::Result<decltype(
          iter::imap(MakeEdgeAccessor, *impl_.OutEdges(edge_types, view)))> {
    auto maybe_edges = impl_.OutEdges(edge_types, view);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  }

  auto OutEdges(storage::View view) const { return OutEdges(view, {}); }

  auto OutEdges(storage::View view,
                const std::vector<storage::EdgeType> &edge_types,
                const VertexAccessor &dest) const
      -> storage::Result<decltype(
          iter::imap(MakeEdgeAccessor, *impl_.OutEdges(edge_types, view)))> {
    auto maybe_edges = impl_.OutEdges(edge_types, view);
    if (maybe_edges.HasError()) return maybe_edges.GetError();
    std::vector<storage::EdgeAccessor> reduced_edges;
    reduced_edges.reserve(maybe_edges->size());
    for (auto &edge : *maybe_edges) {
      if (edge.ToVertex() == dest.impl_) {
        reduced_edges.push_back(edge);
      }
    }
    return iter::imap(MakeEdgeAccessor, std::move(reduced_edges));
  }

  storage::Result<size_t> InDegree(storage::View view) const {
    return impl_.InDegree(view);
  }

  storage::Result<size_t> OutDegree(storage::View view) const {
    return impl_.OutDegree(view);
  }

  int64_t CypherId() const { return impl_.Gid().AsInt(); }

  auto Gid() const { return impl_.Gid(); }

  bool operator==(const VertexAccessor &v) const { return impl_ == v.impl_; }

  bool operator!=(const VertexAccessor &v) const { return !(*this == v); }
};

inline VertexAccessor EdgeAccessor::To() const {
  return VertexAccessor(impl_.ToVertex());
}

inline VertexAccessor EdgeAccessor::From() const {
  return VertexAccessor(impl_.FromVertex());
}

inline bool EdgeAccessor::IsCycle() const { return To() == From(); }
#else

class EdgeAccessor final {
 public:
  ::EdgeAccessor impl_;

 private:
  void SwitchToView(storage::View view) const {
    if (!const_cast<::EdgeAccessor &>(impl_).Reconstruct())
      throw ReconstructionException();
    switch (view) {
      case storage::View::OLD:
        const_cast<::EdgeAccessor &>(impl_).SwitchOld();
        break;
      case storage::View::NEW:
        const_cast<::EdgeAccessor &>(impl_).SwitchNew();
        break;
    }
  }

 public:
  explicit EdgeAccessor(::EdgeAccessor impl) : impl_(impl) {}

  storage::EdgeType EdgeType() const { return impl_.EdgeType(); }

  storage::Result<PropertyValueStore> Properties(storage::View view) const {
    SwitchToView(view);
    return impl_.Properties();
  }

  storage::Result<PropertyValue> GetProperty(storage::View view,
                                             storage::Property key) const {
    SwitchToView(view);
    return impl_.PropsAt(key);
  }

  storage::Result<bool> SetProperty(storage::Property key,
                                    const PropertyValue &value) {
    SwitchToView(storage::View::NEW);
    try {
      impl_.PropsSet(key, value);
      return true;
    } catch (const RecordDeletedError &) {
      return storage::Error::DELETED_OBJECT;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    } catch (const database::ConstraintViolationException &e) {
      throw QueryRuntimeException(e.what());
    }
  }

  storage::Result<bool> RemoveProperty(storage::Property key) {
    SwitchToView(storage::View::NEW);
    try {
      impl_.PropsErase(key);
      return true;
    } catch (const RecordDeletedError &) {
      return storage::Error::DELETED_OBJECT;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    } catch (const database::ConstraintViolationException &e) {
      throw QueryRuntimeException(e.what());
    }
  }

  utils::BasicResult<storage::Error, void> ClearProperties() {
    SwitchToView(storage::View::NEW);
    try {
      impl_.PropsClear();
      return {};
    } catch (const RecordDeletedError &) {
      return storage::Error::DELETED_OBJECT;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    } catch (const database::ConstraintViolationException &e) {
      throw QueryRuntimeException(e.what());
    }
  }

  VertexAccessor To() const;

  VertexAccessor From() const;

  bool IsCycle() const;

  int64_t CypherId() const { return impl_.CypherId(); }

  auto Gid() const { return impl_.gid(); }

  bool operator==(const EdgeAccessor &e) const { return impl_ == e.impl_; }

  bool operator!=(const EdgeAccessor &e) const { return !(*this == e); }
};

class VertexAccessor final {
 public:
  ::VertexAccessor impl_;

 private:
  void SwitchToView(storage::View view) const {
    if (!const_cast<::VertexAccessor &>(impl_).Reconstruct())
      throw ReconstructionException();
    switch (view) {
      case storage::View::OLD:
        const_cast<::VertexAccessor &>(impl_).SwitchOld();
        break;
      case storage::View::NEW:
        const_cast<::VertexAccessor &>(impl_).SwitchNew();
        break;
    }
  }

  static EdgeAccessor MakeEdgeAccessor(const ::EdgeAccessor impl) {
    return EdgeAccessor(impl);
  }

 public:
  explicit VertexAccessor(::VertexAccessor impl) : impl_(impl) {}

  storage::Result<std::vector<storage::Label>> Labels(
      storage::View view) const {
    SwitchToView(view);
    return impl_.labels();
  }

  storage::Result<bool> AddLabel(storage::Label label) {
    SwitchToView(storage::View::NEW);
    try {
      impl_.add_label(label);
      return true;
    } catch (const RecordDeletedError &) {
      return storage::Error::DELETED_OBJECT;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    } catch (const database::ConstraintViolationException &e) {
      throw QueryRuntimeException(e.what());
    }
  }

  storage::Result<bool> RemoveLabel(storage::Label label) {
    SwitchToView(storage::View::NEW);
    try {
      impl_.remove_label(label);
      return true;
    } catch (const RecordDeletedError &) {
      return storage::Error::DELETED_OBJECT;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    }
  }

  storage::Result<bool> HasLabel(storage::View view,
                                 storage::Label label) const {
    SwitchToView(view);
    return impl_.has_label(label);
  }

  storage::Result<PropertyValueStore> Properties(storage::View view) const {
    SwitchToView(view);
    return impl_.Properties();
  }

  storage::Result<PropertyValue> GetProperty(storage::View view,
                                             storage::Property key) const {
    SwitchToView(view);
    return impl_.PropsAt(key);
  }

  storage::Result<bool> SetProperty(storage::Property key,
                                    const PropertyValue &value) {
    SwitchToView(storage::View::NEW);
    try {
      impl_.PropsSet(key, value);
      return true;
    } catch (const RecordDeletedError &) {
      return storage::Error::DELETED_OBJECT;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    } catch (const database::ConstraintViolationException &e) {
      throw QueryRuntimeException(e.what());
    }
  }

  storage::Result<bool> RemoveProperty(storage::Property key) {
    SwitchToView(storage::View::NEW);
    try {
      impl_.PropsErase(key);
      return true;
    } catch (const RecordDeletedError &) {
      return storage::Error::DELETED_OBJECT;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    } catch (const database::ConstraintViolationException &e) {
      throw QueryRuntimeException(e.what());
    }
  }

  utils::BasicResult<storage::Error, void> ClearProperties() {
    SwitchToView(storage::View::NEW);
    try {
      impl_.PropsClear();
      return {};
    } catch (const RecordDeletedError &) {
      return storage::Error::DELETED_OBJECT;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    } catch (const database::ConstraintViolationException &e) {
      throw QueryRuntimeException(e.what());
    }
  }

  auto InEdges(storage::View view) const
      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, impl_.in()))> {
    SwitchToView(view);
    return iter::imap(MakeEdgeAccessor, impl_.in());
  }

  auto InEdges(storage::View view,
               const std::vector<storage::EdgeType> &edge_types) const
      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor,
                                             impl_.in(&edge_types)))> {
    SwitchToView(view);
    return iter::imap(MakeEdgeAccessor, impl_.in(&edge_types));
  }

  auto InEdges(storage::View view,
               const std::vector<storage::EdgeType> &edge_types,
               const VertexAccessor &dest) const
      -> storage::Result<decltype(
          iter::imap(MakeEdgeAccessor, impl_.in(dest.impl_, &edge_types)))> {
    SwitchToView(view);
    return iter::imap(MakeEdgeAccessor, impl_.in(dest.impl_, &edge_types));
  }

  auto OutEdges(storage::View view) const
      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, impl_.out()))> {
    SwitchToView(view);
    return iter::imap(MakeEdgeAccessor, impl_.out());
  }

  auto OutEdges(storage::View view,
                const std::vector<storage::EdgeType> &edge_types) const
      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor,
                                             impl_.out(&edge_types)))> {
    SwitchToView(view);
    return iter::imap(MakeEdgeAccessor, impl_.out(&edge_types));
  }

  auto OutEdges(storage::View view,
                const std::vector<storage::EdgeType> &edge_types,
                const VertexAccessor &dest) const
      -> storage::Result<decltype(
          iter::imap(MakeEdgeAccessor, impl_.out(dest.impl_, &edge_types)))> {
    SwitchToView(view);
    return iter::imap(MakeEdgeAccessor, impl_.out(dest.impl_, &edge_types));
  }

  storage::Result<size_t> InDegree(storage::View view) const {
    SwitchToView(view);
    return impl_.in_degree();
  }

  storage::Result<size_t> OutDegree(storage::View view) const {
    SwitchToView(view);
    return impl_.out_degree();
  }

  int64_t CypherId() const { return impl_.CypherId(); }

  auto Gid() const { return impl_.gid(); }

  bool operator==(const VertexAccessor &v) const { return impl_ == v.impl_; }

  bool operator!=(const VertexAccessor &v) const { return !(*this == v); }
};

inline VertexAccessor EdgeAccessor::To() const {
  return VertexAccessor(impl_.to());
}

inline VertexAccessor EdgeAccessor::From() const {
  return VertexAccessor(impl_.from());
}

inline bool EdgeAccessor::IsCycle() const { return To() == From(); }
#endif

#ifdef MG_SINGLE_NODE_V2
class DbAccessor final {
  storage::Storage::Accessor *accessor_;

  class VerticesIterable final {
    storage::VerticesIterable iterable_;

   public:
    class Iterator final {
      storage::VerticesIterable::Iterator it_;

     public:
      explicit Iterator(storage::VerticesIterable::Iterator it) : it_(it) {}

      VertexAccessor operator*() const { return VertexAccessor(*it_); }

      Iterator &operator++() {
        ++it_;
        return *this;
      }

      bool operator==(const Iterator &other) const { return it_ == other.it_; }

      bool operator!=(const Iterator &other) const { return !(other == *this); }
    };

    explicit VerticesIterable(storage::VerticesIterable iterable)
        : iterable_(std::move(iterable)) {}

    Iterator begin() { return Iterator(iterable_.begin()); }

    Iterator end() { return Iterator(iterable_.end()); }
  };

 public:
  explicit DbAccessor(storage::Storage::Accessor *accessor)
      : accessor_(accessor) {}

  VerticesIterable Vertices(storage::View view) {
    return VerticesIterable(accessor_->Vertices(view));
  }

  VerticesIterable Vertices(storage::View view, storage::Label label) {
    return VerticesIterable(accessor_->Vertices(label, view));
  }

  VerticesIterable Vertices(storage::View view, storage::Label label,
                            storage::Property property,
                            const PropertyValue &value) {
    return VerticesIterable(accessor_->Vertices(label, property, value, view));
  }

  VerticesIterable Vertices(
      storage::View view, storage::Label label, storage::Property property,
      const std::optional<utils::Bound<PropertyValue>> &lower,
      const std::optional<utils::Bound<PropertyValue>> &upper) {
    return VerticesIterable(
        accessor_->Vertices(label, property, lower, upper, view));
  }

  VertexAccessor InsertVertex() {
    return VertexAccessor(accessor_->CreateVertex());
  }

  storage::Result<EdgeAccessor> InsertEdge(VertexAccessor *from,
                                           VertexAccessor *to,
                                           const storage::EdgeType &edge_type) {
    auto maybe_edge =
        accessor_->CreateEdge(&from->impl_, &to->impl_, edge_type);
    if (maybe_edge.HasError())
      return storage::Result<EdgeAccessor>(maybe_edge.GetError());
    return EdgeAccessor(std::move(*maybe_edge));
  }

  storage::Result<bool> RemoveEdge(EdgeAccessor *edge) {
    return accessor_->DeleteEdge(&edge->impl_);
  }

  storage::Result<bool> DetachRemoveVertex(VertexAccessor *vertex_accessor) {
    return accessor_->DetachDeleteVertex(&vertex_accessor->impl_);
  }

  storage::Result<bool> RemoveVertex(VertexAccessor *vertex_accessor) {
    return accessor_->DeleteVertex(&vertex_accessor->impl_);
  }

  storage::Property NameToProperty(const std::string_view &name) {
    // TODO: New storage should work with string_view to avoid needless
    // allocation.
    return accessor_->NameToProperty(std::string(name));
  }

  storage::Label NameToLabel(const std::string_view &name) {
    return accessor_->NameToLabel(std::string(name));
  }

  storage::EdgeType NameToEdgeType(const std::string_view &name) {
    return accessor_->NameToEdgeType(std::string(name));
  }

  const std::string &PropertyToName(storage::Property prop) const {
    return accessor_->PropertyToName(prop);
  }

  const std::string &LabelToName(storage::Label label) const {
    return accessor_->LabelToName(label);
  }

  const std::string &EdgeTypeToName(storage::EdgeType type) const {
    return accessor_->EdgeTypeToName(type);
  }

  void AdvanceCommand() { accessor_->AdvanceCommand(); }

  utils::BasicResult<storage::ExistenceConstraintViolation, void> Commit() {
    return accessor_->Commit();
  }

  void Abort() { accessor_->Abort(); }

  bool LabelIndexExists(storage::Label label) const {
    return accessor_->LabelIndexExists(label);
  }

  bool LabelPropertyIndexExists(storage::Label label,
                                storage::Property prop) const {
    return accessor_->LabelPropertyIndexExists(label, prop);
  }

  int64_t VerticesCount() const { return accessor_->ApproximateVertexCount(); }

  int64_t VerticesCount(storage::Label label) const {
    return accessor_->ApproximateVertexCount(label);
  }

  int64_t VerticesCount(storage::Label label,
                        storage::Property property) const {
    return accessor_->ApproximateVertexCount(label, property);
  }

  int64_t VerticesCount(storage::Label label, storage::Property property,
                        const PropertyValue &value) const {
    return accessor_->ApproximateVertexCount(label, property, value);
  }

  int64_t VerticesCount(
      storage::Label label, storage::Property property,
      const std::optional<utils::Bound<PropertyValue>> &lower,
      const std::optional<utils::Bound<PropertyValue>> &upper) const {
    return accessor_->ApproximateVertexCount(label, property, lower, upper);
  }

  storage::IndicesInfo ListAllIndices() const {
    return accessor_->ListAllIndices();
  }

  storage::ConstraintsInfo ListAllConstraints() const {
    return accessor_->ListAllConstraints();
  }
};
#else
class DbAccessor final {
  database::GraphDbAccessor *dba_;

  template <class TVertices>
  class VerticesIterable final {
    TVertices vertices_;

    class Iterator final {
      decltype(vertices_.begin()) it_;

     public:
      explicit Iterator(decltype(it_) it) : it_(std::move(it)) {}

      VertexAccessor operator*() { return VertexAccessor(*it_); }

      Iterator &operator++() {
        ++it_;
        return *this;
      }

      bool operator==(const Iterator &other) const { return other.it_ == it_; }

      bool operator!=(const Iterator &other) const { return !(other == *this); }
    };

   public:
    explicit VerticesIterable(TVertices vertices)
        : vertices_(std::move(vertices)) {}

    Iterator begin() { return Iterator(vertices_.begin()); }

    Iterator end() { return Iterator(vertices_.end()); }
  };

 public:
  explicit DbAccessor(database::GraphDbAccessor *dba) : dba_(dba) {}

  VertexAccessor InsertVertex() { return VertexAccessor(dba_->InsertVertex()); }

  storage::Result<EdgeAccessor> InsertEdge(VertexAccessor *from,
                                           VertexAccessor *to,
                                           const storage::EdgeType &edge_type) {
    try {
      return EdgeAccessor(dba_->InsertEdge(from->impl_, to->impl_, edge_type));
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    } catch (const RecordDeletedError &) {
      return storage::Error::DELETED_OBJECT;
    }
  }

  storage::Result<bool> RemoveEdge(EdgeAccessor *edge) {
    try {
      dba_->RemoveEdge(edge->impl_);
      return true;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    }
  }

  storage::Result<bool> DetachRemoveVertex(VertexAccessor *vertex_accessor) {
    try {
      dba_->DetachRemoveVertex(vertex_accessor->impl_);
      return true;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    }
  }

  storage::Result<bool> RemoveVertex(VertexAccessor *vertex_accessor) {
    try {
      if (!dba_->RemoveVertex(vertex_accessor->impl_))
        return storage::Error::VERTEX_HAS_EDGES;
      return true;
    } catch (const mvcc::SerializationError &) {
      return storage::Error::SERIALIZATION_ERROR;
    }
  }

  auto Vertices(storage::View view) {
    auto vertices = dba_->Vertices(view == storage::View::NEW);
    return VerticesIterable<decltype(vertices)>(std::move(vertices));
  }

  auto Vertices(storage::View view, storage::Label label) {
    auto vertices = dba_->Vertices(label, view == storage::View::NEW);
    return VerticesIterable<decltype(vertices)>(std::move(vertices));
  }

  auto Vertices(storage::View view, storage::Label label,
                storage::Property property, const PropertyValue &value) {
    auto vertices =
        dba_->Vertices(label, property, value, view == storage::View::NEW);
    return VerticesIterable<decltype(vertices)>(std::move(vertices));
  }

  auto Vertices(storage::View view, storage::Label label,
                storage::Property property,
                const std::optional<utils::Bound<PropertyValue>> &lower,
                const std::optional<utils::Bound<PropertyValue>> &upper) {
    auto vertices = dba_->Vertices(label, property, lower, upper,
                                   view == storage::View::NEW);
    return VerticesIterable<decltype(vertices)>(std::move(vertices));
  }

  storage::Property NameToProperty(const std::string_view &name) {
    return dba_->Property(std::string(name));
  }

  storage::Label NameToLabel(const std::string_view &name) {
    return dba_->Label(std::string(name));
  }

  storage::EdgeType NameToEdgeType(const std::string_view &name) {
    return dba_->EdgeType(std::string(name));
  }

  const std::string &PropertyToName(storage::Property prop) const {
    return dba_->PropertyName(prop);
  }

  const std::string &LabelToName(storage::Label label) const {
    return dba_->LabelName(label);
  }

  const std::string &EdgeTypeToName(storage::EdgeType type) const {
    return dba_->EdgeTypeName(type);
  }

  void AdvanceCommand() { dba_->AdvanceCommand(); }

  bool CreateIndex(storage::Label label, storage::Property prop) {
    try {
      dba_->BuildIndex(label, prop);
      return true;
    } catch (const database::IndexExistsException &) {
      return false;
    } catch (const database::TransactionException &) {
      // TODO: What do we do with this? This cannot happen in v2
      throw;
    }
  }

  bool DropIndex(storage::Label label, storage::Property prop) {
    try {
      dba_->DeleteIndex(label, prop);
      return true;
    } catch (const database::TransactionException &) {
      // TODO: What do we do with this? This cannot happen in v2
      throw;
    }
  }

  bool LabelIndexExists(storage::Label label) const {
    // Label indices exist for all labels in the v1 storage.
    return true;
  }

  bool LabelPropertyIndexExists(storage::Label label,
                                storage::Property prop) const {
    return dba_->LabelPropertyIndexExists(label, prop);
  }

  int64_t VerticesCount() const { return dba_->VerticesCount(); }

  int64_t VerticesCount(storage::Label label) const {
    return dba_->VerticesCount(label);
  }

  int64_t VerticesCount(storage::Label label,
                        storage::Property property) const {
    return dba_->VerticesCount(label, property);
  }

  int64_t VerticesCount(storage::Label label, storage::Property property,
                        const PropertyValue &value) const {
    return dba_->VerticesCount(label, property, value);
  }

  int64_t VerticesCount(
      storage::Label label, storage::Property property,
      const std::optional<utils::Bound<PropertyValue>> &lower,
      const std::optional<utils::Bound<PropertyValue>> &upper) const {
    return dba_->VerticesCount(label, property, lower, upper);
  }

  auto StorageInfo() const { return dba_->StorageInfo(); }

  auto IndexInfo() const { return dba_->IndexInfo(); }

  auto GetIndicesKeys() const { return dba_->GetIndicesKeys(); }

  auto ListUniqueConstraints() const { return dba_->ListUniqueConstraints(); }

  void BuildUniqueConstraint(storage::Label label,
                             const std::vector<storage::Property> &properties) {
    // TODO: Exceptions?
    return dba_->BuildUniqueConstraint(label, properties);
  }

  void DeleteUniqueConstraint(
      storage::Label label, const std::vector<storage::Property> &properties) {
    // TODO: Exceptions?
    return dba_->DeleteUniqueConstraint(label, properties);
  }

#ifdef MG_SINGLE_NODE_HA
  auto raft() { return dba_->raft(); }
#endif
};
#endif

}  // namespace query

namespace std {

template <>
struct hash<query::VertexAccessor> {
  size_t operator()(const query::VertexAccessor &v) const {
    return std::hash<decltype(v.impl_)>{}(v.impl_);
  }
};

template <>
struct hash<query::EdgeAccessor> {
  size_t operator()(const query::EdgeAccessor &e) const {
    return std::hash<decltype(e.impl_)>{}(e.impl_);
  }
};

}  // namespace std
