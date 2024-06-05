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

struct Vertex;
struct VertexBuilder;

struct Edge FLATBUFFERS_FINAL_CLASS : private ::flatbuffers::Table {
  typedef EdgeBuilder Builder;
  enum FlatBuffersVTableOffset FLATBUFFERS_VTABLE_UNDERLYING_TYPE {
    VT_GID = 4,
    VT_TYPE = 6,
    VT_VERTEX_GID = 8,
    VT_PROPERTIES = 10,
    VT_DELETED = 12
  };
  uint64_t gid() const { return GetField<uint64_t>(VT_GID, 0); }
  uint32_t type() const { return GetField<uint32_t>(VT_TYPE, 0); }
  uint64_t vertex_gid() const { return GetField<uint64_t>(VT_VERTEX_GID, 0); }
  const ::flatbuffers::Vector<uint8_t> *properties() const {
    return GetPointer<const ::flatbuffers::Vector<uint8_t> *>(VT_PROPERTIES);
  }
  bool deleted() const { return GetField<uint8_t>(VT_DELETED, 0) != 0; }
  bool Verify(::flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) && VerifyField<uint64_t>(verifier, VT_GID, 8) &&
           VerifyField<uint32_t>(verifier, VT_TYPE, 4) && VerifyField<uint64_t>(verifier, VT_VERTEX_GID, 8) &&
           VerifyOffset(verifier, VT_PROPERTIES) && verifier.VerifyVector(properties()) &&
           VerifyField<uint8_t>(verifier, VT_DELETED, 1) && verifier.EndTable();
  }
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

struct Vertex FLATBUFFERS_FINAL_CLASS : private ::flatbuffers::Table {
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
  const ::flatbuffers::Vector<uint32_t> *labels() const {
    return GetPointer<const ::flatbuffers::Vector<uint32_t> *>(VT_LABELS);
  }
  const ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *in_edges() const {
    return GetPointer<const ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *>(VT_IN_EDGES);
  }
  const ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *out_edges() const {
    return GetPointer<const ::flatbuffers::Vector<::flatbuffers::Offset<disk_exp::Edge>> *>(VT_OUT_EDGES);
  }
  const ::flatbuffers::Vector<uint8_t> *properties() const {
    return GetPointer<const ::flatbuffers::Vector<uint8_t> *>(VT_PROPERTIES);
  }
  bool deleted() const { return GetField<uint8_t>(VT_DELETED, 0) != 0; }
  bool Verify(::flatbuffers::Verifier &verifier) const {
    return VerifyTableStart(verifier) && VerifyField<uint64_t>(verifier, VT_GID, 8) &&
           VerifyOffset(verifier, VT_LABELS) && verifier.VerifyVector(labels()) &&
           VerifyOffset(verifier, VT_IN_EDGES) && verifier.VerifyVector(in_edges()) &&
           verifier.VerifyVectorOfTables(in_edges()) && VerifyOffset(verifier, VT_OUT_EDGES) &&
           verifier.VerifyVector(out_edges()) && verifier.VerifyVectorOfTables(out_edges()) &&
           VerifyOffset(verifier, VT_PROPERTIES) && verifier.VerifyVector(properties()) &&
           VerifyField<uint8_t>(verifier, VT_DELETED, 1) && verifier.EndTable();
  }
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

inline const disk_exp::Vertex *GetVertex(const void *buf) { return ::flatbuffers::GetRoot<disk_exp::Vertex>(buf); }

inline const disk_exp::Vertex *GetSizePrefixedVertex(const void *buf) {
  return ::flatbuffers::GetSizePrefixedRoot<disk_exp::Vertex>(buf);
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

}  // namespace disk_exp

#endif  // FLATBUFFERS_GENERATED_VERTEX_DISK_EXP_H_
