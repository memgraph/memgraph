// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#ifndef FLATBUFFERS_GENERATED_VERTEX_DISK_EXP_H_
#define FLATBUFFERS_GENERATED_VERTEX_DISK_EXP_H_

#include "flatbuffers/flatbuffers.h"

// Ensure the included flatbuffers.h is the same version as when this file was
// generated, otherwise it may not be compatible.
static_assert(FLATBUFFERS_VERSION_MAJOR == 24 && FLATBUFFERS_VERSION_MINOR == 3 && FLATBUFFERS_VERSION_REVISION == 25,
              "Non-compatible flatbuffers version included");

namespace disk_exp {

struct Edge;
struct EdgeBuilder;
struct EdgeT;

struct Vertex;
struct VertexBuilder;
struct VertexT;

struct EdgeT : public ::flatbuffers::NativeTable {
  typedef Edge TableType;
  uint64_t gid = 0;
  uint32_t type = 0;
  uint64_t vertex_gid = 0;
  std::vector<uint8_t> properties{};
  bool deleted = false;
};

struct Edge FLATBUFFERS_FINAL_CLASS : private ::flatbuffers::Table {
  typedef EdgeT NativeTableType;
  typedef EdgeBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_GID = 4,
    VT_TYPE = 6,
    VT_VERTEX_GID = 8,
    VT_PROPERTIES = 10,
    VT_DELETED = 12
  };
  uint64_t gid() const { return GetField<uint64_t>(VT_GID, 0); }
  bool mutate_gid(uint64_t _gid = 0) { return SetField<uint64_t>(VT_GID, _gid, 0); }
  uint32_t type() const { return GetField<uint32_t>(VT_TYPE, 0); }
  bool mutate_type(uint32_t _type = 0) { return SetField<uint32_t>(VT_TYPE, _type, 0); }
  uint64_t vertex_gid() const { return GetField<uint64_t>(VT_VERTEX_GID, 0); }
  bool mutate_vertex_gid(uint64_t _vertex_gid = 0) { return SetField<uint64_t>(VT_VERTEX_GID, _vertex_gid, 0); }
  const ::flatbuffers::Vector<uint8_t> *properties() const {
    return GetPointer<const ::flatbuffers::Vector<uint8_t> *>(VT_PROPERTIES);
  }
  ::flatbuffers::Vector<uint8_t> *mutable_properties() {
    return GetPointer<::flatbuffers::Vector<uint8_t> *>(VT_PROPERTIES);
  }
  bool deleted() const { return GetField<uint8_t>(VT_DELETED, 0) != 0; }
  bool mutate_deleted(bool _deleted = 0) { return SetField<uint8_t>(VT_DELETED, static_cast<uint8_t>(_deleted), 0); }
  bool Verify(::flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) && VerifyField<uint64_t>(verifier, VT_GID, 8) &&
           VerifyField<uint32_t>(verifier, VT_TYPE, 4) && VerifyField<uint64_t>(verifier, VT_VERTEX_GID, 8) &&
           VerifyOffset(verifier, VT_PROPERTIES) && verifier.VerifyVector(properties()) &&
           VerifyField<uint8_t>(verifier, VT_DELETED, 1) && verifier.EndTable();
  }
  EdgeT *UnPack(const ::flatbuffers::resolver_function_t *_resolver = nullptr) const;
  void UnPackTo(EdgeT *_o, const ::flatbuffers::resolver_function_t *_resolver = nullptr) const;
  static ::flatbuffers::Offset<Edge> Pack(::flatbuffers::FlatBufferBuilder &_fbb, const EdgeT *_o,
                                          const ::flatbuffers::rehasher_function_t *_rehasher = nullptr);
};

struct EdgeBuilder {
  typedef Edge Table;
  ::flatbuffers::FlatBufferBuilder &fbb_;
  ::flatbuffers::uoffset_t start_;
  void add_gid(uint64_t gid) { fbb_.AddElement<uint64_t>(Edge::VT_GID, gid, 0); }
  void add_type(uint32_t type) { fbb_.AddElement<uint32_t>(Edge::VT_TYPE, type, 0); }
  void add_vertex_gid(uint64_t vertex_gid) { fbb_.AddElement<uint64_t>(Edge::VT_VERTEX_GID, vertex_gid, 0); }
  void add_properties(::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> properties) {
    fbb_.AddOffset(Edge::VT_PROPERTIES, properties);
  }
  void add_deleted(bool deleted) { fbb_.AddElement<uint8_t>(Edge::VT_DELETED, static_cast<uint8_t>(deleted), 0); }
  explicit EdgeBuilder(::flatbuffers::FlatBufferBuilder &_fbb) : fbb_(_fbb) { start_ = fbb_.StartTable(); }
  ::flatbuffers::Offset<Edge> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = ::flatbuffers::Offset<Edge>(end);
    return o;
  }
};

inline ::flatbuffers::Offset<Edge> CreateEdge(::flatbuffers::FlatBufferBuilder &_fbb, uint64_t gid = 0,
                                              uint32_t type = 0, uint64_t vertex_gid = 0,
                                              ::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> properties = 0,
                                              bool deleted = false) {
  EdgeBuilder builder_(_fbb);
  builder_.add_vertex_gid(vertex_gid);
  builder_.add_gid(gid);
  builder_.add_properties(properties);
  builder_.add_type(type);
  builder_.add_deleted(deleted);
  return builder_.Finish();
}

inline ::flatbuffers::Offset<Edge> CreateEdgeDirect(::flatbuffers::FlatBufferBuilder &_fbb, uint64_t gid = 0,
                                                    uint32_t type = 0, uint64_t vertex_gid = 0,
                                                    const std::vector<uint8_t> *properties = nullptr,
                                                    bool deleted = false) {
  auto properties__ = properties ? _fbb.CreateVector<uint8_t>(*properties) : 0;
  return disk_exp::CreateEdge(_fbb, gid, type, vertex_gid, properties__, deleted);
}

::flatbuffers::Offset<Edge> CreateEdge(::flatbuffers::FlatBufferBuilder &_fbb, const EdgeT *_o,
                                       const ::flatbuffers::rehasher_function_t *_rehasher = nullptr);

struct VertexT : public ::flatbuffers::NativeTable {
  typedef Vertex TableType;
  uint64_t gid = 0;
  std::vector<uint32_t> labels{};
  std::vector<std::unique_ptr<disk_exp::EdgeT>> in_edges{};
  std::vector<std::unique_ptr<disk_exp::EdgeT>> out_edges{};
  std::vector<uint8_t> properties{};
  bool deleted = false;
  VertexT() = default;
  VertexT(const VertexT &o);
  VertexT(VertexT &&) FLATBUFFERS_NOEXCEPT = default;
  VertexT &operator=(VertexT o) FLATBUFFERS_NOEXCEPT;
};

struct Vertex FLATBUFFERS_FINAL_CLASS : private ::flatbuffers::Table {
  typedef VertexT NativeTableType;
  typedef VertexBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_GID = 4,
    VT_LABELS = 6,
    VT_IN_EDGES = 8,
    VT_OUT_EDGES = 10,
    VT_PROPERTIES = 12,
    VT_DELETED = 14
  };
  uint64_t gid() const { return GetField<uint64_t>(VT_GID, 0); }
  bool mutate_gid(uint64_t _gid = 0) { return SetField<uint64_t>(VT_GID, _gid, 0); }
  const ::flatbuffers::Vector<uint32_t> *labels() const {
    return GetPointer<const ::flatbuffers::Vector<uint32_t> *>(VT_LABELS);
  }
  ::flatbuffers::Vector<uint32_t> *mutable_labels() { return GetPointer<::flatbuffers::Vector<uint32_t> *>(VT_LABELS); }
  const ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *in_edges() const {
    return GetPointer<const ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *>(VT_IN_EDGES);
  }
  ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *mutable_in_edges() {
    return GetPointer<::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *>(VT_IN_EDGES);
  }
  const ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *out_edges() const {
    return GetPointer<const ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *>(VT_OUT_EDGES);
  }
  ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *mutable_out_edges() {
    return GetPointer<::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *>(VT_OUT_EDGES);
  }
  const ::flatbuffers::Vector<uint8_t> *properties() const {
    return GetPointer<const ::flatbuffers::Vector<uint8_t> *>(VT_PROPERTIES);
  }
  ::flatbuffers::Vector<uint8_t> *mutable_properties() {
    return GetPointer<::flatbuffers::Vector<uint8_t> *>(VT_PROPERTIES);
  }
  bool deleted() const { return GetField<uint8_t>(VT_DELETED, 0) != 0; }
  bool mutate_deleted(bool _deleted = 0) { return SetField<uint8_t>(VT_DELETED, static_cast<uint8_t>(_deleted), 0); }
  bool Verify(::flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) && VerifyField<uint64_t>(verifier, VT_GID, 8) &&
           VerifyOffset(verifier, VT_LABELS) && verifier.VerifyVector(labels()) &&
           VerifyOffset(verifier, VT_IN_EDGES) && verifier.VerifyVector(in_edges()) &&
           verifier.VerifyVectorOfTables(in_edges()) && VerifyOffset(verifier, VT_OUT_EDGES) &&
           verifier.VerifyVector(out_edges()) && verifier.VerifyVectorOfTables(out_edges()) &&
           VerifyOffset(verifier, VT_PROPERTIES) && verifier.VerifyVector(properties()) &&
           VerifyField<uint8_t>(verifier, VT_DELETED, 1) && verifier.EndTable();
  }
  VertexT *UnPack(const ::flatbuffers::resolver_function_t *_resolver = nullptr) const;
  void UnPackTo(VertexT *_o, const ::flatbuffers::resolver_function_t *_resolver = nullptr) const;
  static ::flatbuffers::Offset<Vertex> Pack(::flatbuffers::FlatBufferBuilder &_fbb, const VertexT *_o,
                                            const ::flatbuffers::rehasher_function_t *_rehasher = nullptr);
};

struct VertexBuilder {
  typedef Vertex Table;
  ::flatbuffers::FlatBufferBuilder &fbb_;
  ::flatbuffers::uoffset_t start_;
  void add_gid(uint64_t gid) { fbb_.AddElement<uint64_t>(Vertex::VT_GID, gid, 0); }
  void add_labels(::flatbuffers::Offset<::flatbuffers::Vector<uint32_t>> labels) {
    fbb_.AddOffset(Vertex::VT_LABELS, labels);
  }
  void add_in_edges(::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>>> in_edges) {
    fbb_.AddOffset(Vertex::VT_IN_EDGES, in_edges);
  }
  void add_out_edges(::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>>> out_edges) {
    fbb_.AddOffset(Vertex::VT_OUT_EDGES, out_edges);
  }
  void add_properties(::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> properties) {
    fbb_.AddOffset(Vertex::VT_PROPERTIES, properties);
  }
  void add_deleted(bool deleted) { fbb_.AddElement<uint8_t>(Vertex::VT_DELETED, static_cast<uint8_t>(deleted), 0); }
  explicit VertexBuilder(::flatbuffers::FlatBufferBuilder &_fbb) : fbb_(_fbb) { start_ = fbb_.StartTable(); }
  ::flatbuffers::Offset<Vertex> Finish() {
    const auto end = fbb_.EndTable(start_);
    auto o = ::flatbuffers::Offset<Vertex>(end);
    return o;
  }
};

inline ::flatbuffers::Offset<Vertex> CreateVertex(
    ::flatbuffers::FlatBufferBuilder &_fbb, uint64_t gid = 0,
    ::flatbuffers::Offset<::flatbuffers::Vector<uint32_t>> labels = 0,
    ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>>> in_edges = 0,
    ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>>> out_edges = 0,
    ::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> properties = 0, bool deleted = false) {
  VertexBuilder builder_(_fbb);
  builder_.add_gid(gid);
  builder_.add_properties(properties);
  builder_.add_out_edges(out_edges);
  builder_.add_in_edges(in_edges);
  builder_.add_labels(labels);
  builder_.add_deleted(deleted);
  return builder_.Finish();
}

inline ::flatbuffers::Offset<Vertex> CreateVertexDirect(
    ::flatbuffers::FlatBufferBuilder &_fbb, uint64_t gid = 0, const std::vector<uint32_t> *labels = nullptr,
    const std::vector<::flatbuffers::Offset<disk_exp::Edge>> *in_edges = nullptr,
    const std::vector<::flatbuffers::Offset<disk_exp::Edge>> *out_edges = nullptr,
    const std::vector<uint8_t> *properties = nullptr, bool deleted = false) {
  auto labels__ = labels ? _fbb.CreateVector<uint32_t>(*labels) : 0;
  auto in_edges__ = in_edges ? _fbb.CreateVector<::flatbuffers::Offset<disk_exp::Edge>>(*in_edges) : 0;
  auto out_edges__ = out_edges ? _fbb.CreateVector<::flatbuffers::Offset<disk_exp::Edge>>(*out_edges) : 0;
  auto properties__ = properties ? _fbb.CreateVector<uint8_t>(*properties) : 0;
  return disk_exp::CreateVertex(_fbb, gid, labels__, in_edges__, out_edges__, properties__, deleted);
}

::flatbuffers::Offset<Vertex> CreateVertex(::flatbuffers::FlatBufferBuilder &_fbb, const VertexT *_o,
                                           const ::flatbuffers::rehasher_function_t *_rehasher = nullptr);

inline EdgeT *Edge::UnPack(const ::flatbuffers::resolver_function_t *_resolver) const {
  auto _o = std::unique_ptr<EdgeT>(new EdgeT());
  UnPackTo(_o.get(), _resolver);
  return _o.release();
}

inline void Edge::UnPackTo(EdgeT *_o, const ::flatbuffers::resolver_function_t *_resolver) const {
  (void)_o;
  (void)_resolver;
  {
    auto _e = gid();
    _o->gid = _e;
  }
  {
    auto _e = type();
    _o->type = _e;
  }
  {
    auto _e = vertex_gid();
    _o->vertex_gid = _e;
  }
  {
    auto _e = properties();
    if (_e) {
      _o->properties.resize(_e->size());
      std::copy(_e->begin(), _e->end(), _o->properties.begin());
    }
  }
  {
    auto _e = deleted();
    _o->deleted = _e;
  }
}

inline ::flatbuffers::Offset<Edge> Edge::Pack(::flatbuffers::FlatBufferBuilder &_fbb, const EdgeT *_o,
                                              const ::flatbuffers::rehasher_function_t *_rehasher) {
  return CreateEdge(_fbb, _o, _rehasher);
}

inline ::flatbuffers::Offset<Edge> CreateEdge(::flatbuffers::FlatBufferBuilder &_fbb, const EdgeT *_o,
                                              const ::flatbuffers::rehasher_function_t *_rehasher) {
  (void)_rehasher;
  (void)_o;
  struct _VectorArgs {
    ::flatbuffers::FlatBufferBuilder *__fbb;
    const EdgeT *__o;
    const ::flatbuffers::rehasher_function_t *__rehasher;
  } _va = {&_fbb, _o, _rehasher};
  (void)_va;
  auto _gid = _o->gid;
  auto _type = _o->type;
  auto _vertex_gid = _o->vertex_gid;
  auto _properties = _o->properties.size() ? _fbb.CreateVector(_o->properties) : 0;
  auto _deleted = _o->deleted;
  return disk_exp::CreateEdge(_fbb, _gid, _type, _vertex_gid, _properties, _deleted);
}

inline VertexT::VertexT(const VertexT &o) : gid(o.gid), labels(o.labels), properties(o.properties), deleted(o.deleted) {
  in_edges.reserve(o.in_edges.size());
  for (const auto &in_edges_ : o.in_edges) {
    in_edges.emplace_back((in_edges_) ? new disk_exp::EdgeT(*in_edges_) : nullptr);
  }
  out_edges.reserve(o.out_edges.size());
  for (const auto &out_edges_ : o.out_edges) {
    out_edges.emplace_back((out_edges_) ? new disk_exp::EdgeT(*out_edges_) : nullptr);
  }
}

inline VertexT &VertexT::operator=(VertexT o) FLATBUFFERS_NOEXCEPT {
  std::swap(gid, o.gid);
  std::swap(labels, o.labels);
  std::swap(in_edges, o.in_edges);
  std::swap(out_edges, o.out_edges);
  std::swap(properties, o.properties);
  std::swap(deleted, o.deleted);
  return *this;
}

inline VertexT *Vertex::UnPack(const ::flatbuffers::resolver_function_t *_resolver) const {
  auto _o = std::unique_ptr<VertexT>(new VertexT());
  UnPackTo(_o.get(), _resolver);
  return _o.release();
}

inline void Vertex::UnPackTo(VertexT *_o, const ::flatbuffers::resolver_function_t *_resolver) const {
  (void)_o;
  (void)_resolver;
  {
    auto _e = gid();
    _o->gid = _e;
  }
  {
    auto _e = labels();
    if (_e) {
      _o->labels.resize(_e->size());
      for (::flatbuffers::uoffset_t _i = 0; _i < _e->size(); _i++) {
        _o->labels[_i] = _e->Get(_i);
      }
    } else {
      _o->labels.resize(0);
    }
  }
  {
    auto _e = in_edges();
    if (_e) {
      _o->in_edges.resize(_e->size());
      for (::flatbuffers::uoffset_t _i = 0; _i < _e->size(); _i++) {
        if (_o->in_edges[_i]) {
          _e->Get(_i)->UnPackTo(_o->in_edges[_i].get(), _resolver);
        } else {
          _o->in_edges[_i] = std::unique_ptr<disk_exp::EdgeT>(_e->Get(_i)->UnPack(_resolver));
        };
      }
    } else {
      _o->in_edges.resize(0);
    }
  }
  {
    auto _e = out_edges();
    if (_e) {
      _o->out_edges.resize(_e->size());
      for (::flatbuffers::uoffset_t _i = 0; _i < _e->size(); _i++) {
        if (_o->out_edges[_i]) {
          _e->Get(_i)->UnPackTo(_o->out_edges[_i].get(), _resolver);
        } else {
          _o->out_edges[_i] = std::unique_ptr<disk_exp::EdgeT>(_e->Get(_i)->UnPack(_resolver));
        };
      }
    } else {
      _o->out_edges.resize(0);
    }
  }
  {
    auto _e = properties();
    if (_e) {
      _o->properties.resize(_e->size());
      std::copy(_e->begin(), _e->end(), _o->properties.begin());
    }
  }
  {
    auto _e = deleted();
    _o->deleted = _e;
  }
}

inline ::flatbuffers::Offset<Vertex> Vertex::Pack(::flatbuffers::FlatBufferBuilder &_fbb, const VertexT *_o,
                                                  const ::flatbuffers::rehasher_function_t *_rehasher) {
  return CreateVertex(_fbb, _o, _rehasher);
}

inline ::flatbuffers::Offset<Vertex> CreateVertex(::flatbuffers::FlatBufferBuilder &_fbb, const VertexT *_o,
                                                  const ::flatbuffers::rehasher_function_t *_rehasher) {
  (void)_rehasher;
  (void)_o;
  struct _VectorArgs {
    ::flatbuffers::FlatBufferBuilder *__fbb;
    const VertexT *__o;
    const ::flatbuffers::rehasher_function_t *__rehasher;
  } _va = {&_fbb, _o, _rehasher};
  (void)_va;
  auto _gid = _o->gid;
  auto _labels = _o->labels.size() ? _fbb.CreateVector(_o->labels) : 0;
  auto _in_edges = _o->in_edges.size()
                       ? _fbb.CreateVector<::flatbuffers::Offset<disk_exp::Edge>>(
                             _o->in_edges.size(),
                             [](size_t i, _VectorArgs *__va) {
                               return CreateEdge(*__va->__fbb, __va->__o->in_edges[i].get(), __va->__rehasher);
                             },
                             &_va)
                       : 0;
  auto _out_edges = _o->out_edges.size()
                        ? _fbb.CreateVector<::flatbuffers::Offset<disk_exp::Edge>>(
                              _o->out_edges.size(),
                              [](size_t i, _VectorArgs *__va) {
                                return CreateEdge(*__va->__fbb, __va->__o->out_edges[i].get(), __va->__rehasher);
                              },
                              &_va)
                        : 0;
  auto _properties = _o->properties.size() ? _fbb.CreateVector(_o->properties) : 0;
  auto _deleted = _o->deleted;
  return disk_exp::CreateVertex(_fbb, _gid, _labels, _in_edges, _out_edges, _properties, _deleted);
}

inline const disk_exp::Vertex *GetVertex(const void *buf) { return ::flatbuffers::GetRoot<disk_exp::Vertex>(buf); }

inline const disk_exp::Vertex *GetSizePrefixedVertex(const void *buf) {
  return ::flatbuffers::GetSizePrefixedRoot<disk_exp::Vertex>(buf);
}

inline Vertex *GetMutableVertex(void *buf) { return ::flatbuffers::GetMutableRoot<Vertex>(buf); }

inline disk_exp::Vertex *GetMutableSizePrefixedVertex(void *buf) {
  return ::flatbuffers::GetMutableSizePrefixedRoot<disk_exp::Vertex>(buf);
}

inline bool VerifyVertexBuffer(::flatbuffers::Verifier &verifier) {
  return verifier.VerifyBuffer<disk_exp::Vertex>(nullptr);
}

inline bool VerifySizePrefixedVertexBuffer(::flatbuffers::Verifier &verifier) {
  return verifier.VerifySizePrefixedBuffer<disk_exp::Vertex>(nullptr);
}

inline void FinishVertexBuffer(::flatbuffers::FlatBufferBuilder &fbb, ::flatbuffers::Offset<disk_exp::Vertex> root) {
  fbb.Finish(root);
}

inline void FinishSizePrefixedVertexBuffer(::flatbuffers::FlatBufferBuilder &fbb,
                                           ::flatbuffers::Offset<disk_exp::Vertex> root) {
  fbb.FinishSizePrefixed(root);
}

inline std::unique_ptr<disk_exp::VertexT> UnPackVertex(const void *buf,
                                                       const ::flatbuffers::resolver_function_t *res = nullptr) {
  return std::unique_ptr<disk_exp::VertexT>(GetVertex(buf)->UnPack(res));
}

inline std::unique_ptr<disk_exp::VertexT> UnPackSizePrefixedVertex(
    const void *buf, const ::flatbuffers::resolver_function_t *res = nullptr) {
  return std::unique_ptr<disk_exp::VertexT>(GetSizePrefixedVertex(buf)->UnPack(res));
}

}  // namespace disk_exp

#endif  // FLATBUFFERS_GENERATED_VERTEX_DISK_EXP_H_
