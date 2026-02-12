// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <atomic>
#include <cstdint>

#include "storage/v2/delta_action.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/allocator/page_slab_memory_resource.hpp"
#include "utils/logging.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::storage {

// Forward declarations because we only store pointers here.
struct Vertex;
struct Edge;
struct Delta;

// Communicate across transactions when a transaction has marked another's
// deltas as non-sequential. When PENDING, a transaction knows that it must
// participate in resetting the vertex's `has_uncommitted_non_sequential_deltas`
// flag. When HANDLED, other transactions know that transaction has begun
// cleaning-up and there is no point propagating the flag to existing deltas.
enum class NonSeqPropagationState : uint8_t { NONE, PENDING, HANDLED };

struct CommitInfo {
  explicit CommitInfo(uint64_t ts) : timestamp(ts) {}

  std::atomic<uint64_t> timestamp;

  // Checked by the owning transition during finalization to see if another
  // transaction has set upgraded one of ours deltas to non-sequential.
  utils::SpinLock lock;
  NonSeqPropagationState non_seq_propagation{NonSeqPropagationState::NONE};
};

// This class stores one of three pointers (`Delta`, `Vertex` and `Edge`)
// without using additional memory for storing the type. The type is stored in
// the pointer itself in the lower bits. All of those structures contain large
// items in themselves (e.g. `uint64_t`) that require the pointer to be aligned
// to their size (for `uint64_t` it is 8). That means that the pointer will
// always be a multiple of 8 which implies that the lower 3 bits of the pointer
// will always be 0. We can use those 3 bits to store information about the type
// of the pointer stored (2 bits).
class PreviousPtr {
  static constexpr uintptr_t kDelta = 0b01UL;
  static constexpr uintptr_t kVertex = 0b10UL;
  static constexpr uintptr_t kEdge = 0b11UL;

  static constexpr uintptr_t kMask = 0b11UL;

 public:
  enum class Type {
    NULL_PTR,
    DELTA,
    VERTEX,
    EDGE,
  };

  struct Pointer {
    Pointer() = default;

    explicit Pointer(Delta *delta) : type(Type::DELTA), delta(delta) {}

    explicit Pointer(Vertex *vertex) : type(Type::VERTEX), vertex(vertex) {}

    explicit Pointer(Edge *edge) : type(Type::EDGE), edge(edge) {}

    Type type{Type::NULL_PTR};

    union {
      Delta *delta = nullptr;
      Vertex *vertex;
      Edge *edge;
    };
  };

  PreviousPtr() : storage_(0) {}

  PreviousPtr(const PreviousPtr &other) noexcept : storage_(other.storage_.load(std::memory_order_acquire)) {}

  Pointer Get() const {
    uintptr_t value = storage_.load(std::memory_order_acquire);
    if (value == 0) {
      return {};
    }
    uintptr_t type = value & kMask;
    if (type == kDelta) {
      return Pointer{reinterpret_cast<Delta *>(value & ~kMask)};
    } else if (type == kVertex) {
      return Pointer{reinterpret_cast<Vertex *>(value & ~kMask)};
    } else if (type == kEdge) {
      return Pointer{reinterpret_cast<Edge *>(value & ~kMask)};
    } else {
      LOG_FATAL("Invalid pointer type!");
    }
  }

  void Set(Delta *delta) {
    uintptr_t value = reinterpret_cast<uintptr_t>(delta);
    MG_ASSERT((value & kMask) == 0, "Invalid pointer!");
    storage_.store(value | kDelta, std::memory_order_release);
  }

  void Set(Vertex *vertex) {
    uintptr_t value = reinterpret_cast<uintptr_t>(vertex);
    MG_ASSERT((value & kMask) == 0, "Invalid pointer!");
    storage_.store(value | kVertex, std::memory_order_release);
  }

  void Set(Edge *edge) {
    uintptr_t value = reinterpret_cast<uintptr_t>(edge);
    MG_ASSERT((value & kMask) == 0, "Invalid pointer!");
    storage_.store(value | kEdge, std::memory_order_release);
  }

 private:
  std::atomic<uintptr_t> storage_;
};

inline bool operator==(const PreviousPtr::Pointer &a, const PreviousPtr::Pointer &b) {
  if (a.type != b.type) return false;
  switch (a.type) {
    case PreviousPtr::Type::VERTEX:
      return a.vertex == b.vertex;
    case PreviousPtr::Type::EDGE:
      return a.edge == b.edge;
    case PreviousPtr::Type::DELTA:
      return a.delta == b.delta;
    case PreviousPtr::Type::NULL_PTR:
      return b.type == PreviousPtr::Type::NULL_PTR;
  }
}

/// Generic pointer packing: stores a pointer of type \p T with low bits used for flags.
/// The pointer must be aligned so the low \p NumFlagBits are zero (e.g. 8-byte aligned ⇒ 3 bits free).
template <typename T, int NumFlagBits>
class PointerPack {
  static_assert(NumFlagBits > 0 && NumFlagBits <= static_cast<int>(8 * sizeof(uintptr_t)));
  static constexpr uintptr_t kFlagsMask = (1UL << NumFlagBits) - 1;
  static constexpr uintptr_t kPtrMask = ~kFlagsMask;

  uintptr_t storage_{0};

 public:
  PointerPack() = default;

  explicit PointerPack(T *ptr, uintptr_t flags = 0)
      : storage_(reinterpret_cast<uintptr_t>(ptr) | (flags & kFlagsMask)) {
    MG_ASSERT((reinterpret_cast<uintptr_t>(ptr) & kFlagsMask) == 0, "Pointer must be aligned!");
  }

  T *get_ptr() const { return reinterpret_cast<T *>(storage_ & kPtrMask); }

  /// Extracts the bit field at position \p Pos with \p Size bits (0-based bit index, \p Size in bits).
  template <int Pos, int Size = 1>
  uintptr_t get() const {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    return (storage_ >> Pos) & ((1UL << Size) - 1);
  }

  /// Sets the bit field at position \p Pos with \p Size bits to \p value.
  template <int Pos, int Size = 1>
  void set(uintptr_t value) {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    const uintptr_t field_mask = ((1UL << Size) - 1) << Pos;
    storage_ = (storage_ & ~field_mask) | ((value << Pos) & field_mask);
  }

  void set_ptr(T *ptr) {
    MG_ASSERT((reinterpret_cast<uintptr_t>(ptr) & kFlagsMask) == 0, "Pointer must be aligned!");
    storage_ = reinterpret_cast<uintptr_t>(ptr) | (storage_ & kFlagsMask);
  }

  operator T *() const { return get_ptr(); }

  PointerPack &operator=(T *ptr) {
    set_ptr(ptr);
    return *this;
  }
};

/// Packs the \c deleted flag and (on Vertex) \c has_uncommitted_non_sequential_deltas into the low bits
/// of the Delta* pointer (8-byte aligned, so bits 0–2 are free). Uses PointerPack with bit 0 = deleted,
/// bit 1 = has_uncommitted_non_sequential_deltas.
using DeltaPtrPack = PointerPack<Delta, 2>;

namespace delta_ptr_pack {
constexpr int kDeletedBit = 0;
constexpr int kHasUncommittedNonSeqDeltasBit = 1;
}  // namespace delta_ptr_pack

inline Delta *get(DeltaPtrPack const &p) { return p.get_ptr(); }

inline bool deleted(DeltaPtrPack const &p) { return p.get<delta_ptr_pack::kDeletedBit, 1>() != 0; }

inline void set_deleted(DeltaPtrPack &p, bool b) { p.set<delta_ptr_pack::kDeletedBit, 1>(b ? 1 : 0); }

inline bool has_uncommitted_non_sequential_deltas(DeltaPtrPack const &p) {
  return p.get<delta_ptr_pack::kHasUncommittedNonSeqDeltasBit, 1>() != 0;
}

inline void set_has_uncommitted_non_sequential_deltas(DeltaPtrPack &p, bool b) {
  p.set<delta_ptr_pack::kHasUncommittedNonSeqDeltasBit, 1>(b ? 1 : 0);
}

inline void set_delta(DeltaPtrPack &p, Delta *d) { p.set_ptr(d); }

struct opt_str {
  opt_str(std::optional<std::string_view> other, utils::PageSlabMemoryResource *res)
      : str_{other ? new_cstr(*other, res) : nullptr} {}

  ~opt_str() = default;

  auto as_opt_str() const -> std::optional<std::string_view> {
    if (!str_) return std::nullopt;
    return std::optional<std::string_view>{std::in_place, str_};
  }

 private:
  static auto new_cstr(std::string_view str, utils::PageSlabMemoryResource *res) -> char const * {
    auto const n = str.size() + 1;
    auto alloc = std::pmr::polymorphic_allocator<char>{res};
    auto *mem = (std::string_view::pointer)alloc.allocate_bytes(n, alignof(char));
    std::copy(str.cbegin(), str.cend(), mem);
    mem[n - 1] = '\0';
    return mem;
  }

  char const *str_ = nullptr;
};

static_assert(!std::is_constructible_v<opt_str, std::optional<std::string_view>, std::pmr::memory_resource *>,
              "Use of PageSlabMemoryResource is deliberate here");
static_assert(std::is_constructible_v<opt_str, std::optional<std::string_view>, utils::PageSlabMemoryResource *>,
              "Use of PageSlabMemoryResource is deliberate here");
static_assert(std::is_trivially_destructible_v<opt_str>,
              "uses PageSlabMemoryResource, lifetime linked to that, dtr should be trivial");

/* Non-sequential deltas relax the usual rules of MVCC by allowing certain
 * operations (i.e., edge creations) to be prepended to a delta chain in a
 * non-sequential manner, even in cases where normally this would be a write
 * conflict. Doing so hugely improves performance in edge-write heavy imports,
 * at the expense of increasing delta chain iteration and garbage collection
 * whilst non-sequential deltas exist in the chains. */
enum class DeltaSequencing : uint8_t { SEQUENTIAL, NON_SEQUENTIAL };

enum class DeltaChainState : uint8_t {
  SEQUENTIAL = 0,        // Normal MVCC delta which stop traversal at transaction boundaries
  NON_SEQUENTIAL = 1,    // Can traverse past other transactions' uncommitted edge deltas
  FORCED_SEQUENTIAL = 2  // Has blocking operations upstream, preventing non-sequential writes
};

/**
 * By using a tagged pointer for the vertex in a vertex_edge `Delta`, we can
 * store flags without increasing the size of the overall `Delta`:
 *
 */
class TaggedVertexPtr {
 public:
  TaggedVertexPtr() : ptr_(nullptr) {}

  TaggedVertexPtr(Vertex *vertex, DeltaChainState state = DeltaChainState::SEQUENTIAL) { Set(vertex, state); }

  TaggedVertexPtr(TaggedVertexPtr const &) = delete;
  TaggedVertexPtr(TaggedVertexPtr &&) = delete;
  TaggedVertexPtr &operator=(TaggedVertexPtr const &) = delete;
  TaggedVertexPtr &operator=(TaggedVertexPtr &&) = delete;

  // TODO(colinbarry) These member functions should use bit_cast, but
  // the v7 toolchain's version of clang-tidy crashes when we use that. For
  // now, we stick with reinterpret_casts.

  Vertex *Get() const {
    auto ptr_value = reinterpret_cast<uintptr_t>(ptr_.load(std::memory_order_acquire)) & ~0x3UL;
    return std::bit_cast<Vertex *>(ptr_value);
  }

  Vertex *operator->() const { return Get(); }

  DeltaChainState GetState() const {
    auto flags = reinterpret_cast<uintptr_t>(ptr_.load(std::memory_order_acquire)) & 0x3UL;
    return static_cast<DeltaChainState>(flags);
  }

  void Set(Vertex *vertex, DeltaChainState state = DeltaChainState::SEQUENTIAL) {
    auto vertex_ptr = std::bit_cast<uintptr_t>(vertex);
    vertex_ptr |= static_cast<uintptr_t>(state);
    ptr_.store(reinterpret_cast<Vertex *>(vertex_ptr), std::memory_order_release);
  }

  void SetState(DeltaChainState state) {
    // Safe to perform updates non-atomically as this is only called with
    // the vertex under lock.
    auto old_value = reinterpret_cast<uintptr_t>(ptr_.load(std::memory_order_acquire));
    auto new_value = (old_value & ~0x3UL) | static_cast<uintptr_t>(state);
    ptr_.store(std::bit_cast<Vertex *>(new_value), std::memory_order_release);
  }

 private:
  std::atomic<Vertex *> ptr_;
};

struct Delta {
  using Action = DeltaAction;

  // Used for both Vertex and Edge
  struct DeleteDeserializedObjectTag {};

  struct DeleteObjectTag {};

  struct RecreateObjectTag {};

  struct SetPropertyTag {};

  // Used only for Vertex
  struct AddLabelTag {};

  struct RemoveLabelTag {};

  struct AddInEdgeTag {};

  struct AddOutEdgeTag {};

  struct RemoveInEdgeTag {};

  struct RemoveOutEdgeTag {};

  // DELETE_DESERIALIZED_OBJECT is used to load data from disk committed by past txs.
  // Because of this object was created in past txs, we create timestamp by ourselves inside instead of having it from
  // current tx. This timestamp we got from RocksDB timestamp stored in key.
  Delta(DeleteDeserializedObjectTag /*tag*/, uint64_t ts, std::optional<std::string_view> old_disk_key,
        utils::PageSlabMemoryResource *res)
      : commit_info(std::pmr::polymorphic_allocator<Delta>{res}.new_object<CommitInfo>(ts)),
        command_id(0),
        old_disk_key{.value = opt_str{old_disk_key, res}} {}

  Delta(DeleteObjectTag /*tag*/, CommitInfo *commit_info, uint64_t command_id)
      : commit_info(commit_info), command_id(command_id), action(Action::DELETE_OBJECT) {}

  Delta(RecreateObjectTag /*tag*/, CommitInfo *commit_info, uint64_t command_id)
      : commit_info(commit_info), command_id(command_id), action(Action::RECREATE_OBJECT) {}

  Delta(AddLabelTag /*tag*/, LabelId label, CommitInfo *commit_info, uint64_t command_id)
      : commit_info(commit_info), command_id(command_id), label{.action = Action::ADD_LABEL, .value = label} {}

  Delta(RemoveLabelTag /*tag*/, LabelId label, CommitInfo *commit_info, uint64_t command_id)
      : commit_info(commit_info), command_id(command_id), label{.action = Action::REMOVE_LABEL, .value = label} {}

  Delta(SetPropertyTag /*tag*/, PropertyId key, PropertyValue const &value, CommitInfo *commit_info,
        uint64_t command_id, utils::PageSlabMemoryResource *res)
      : commit_info(commit_info),
        command_id(command_id),
        property{.action = Action::SET_PROPERTY,
                 .key = key,
                 .value = std::pmr::polymorphic_allocator<Delta>{res}.new_object<pmr::PropertyValue>(value)} {}

  Delta(SetPropertyTag /*tag*/, Vertex *out_vertex, PropertyId key, PropertyValue value, CommitInfo *commit_info,
        uint64_t command_id, utils::PageSlabMemoryResource *res)
      : commit_info(commit_info),
        command_id(command_id),
        property{.action = Action::SET_PROPERTY,
                 .key = key,
                 .value = std::pmr::polymorphic_allocator<Delta>{res}.new_object<pmr::PropertyValue>(std::move(value)),
                 .out_vertex = out_vertex} {}

  Delta(AddInEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, CommitInfo *commit_info,
        uint64_t command_id)
      : Delta(AddInEdgeTag{}, edge_type, vertex, edge, DeltaSequencing::SEQUENTIAL, commit_info, command_id) {}

  Delta(AddInEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, DeltaSequencing sequencing,
        CommitInfo *commit_info, uint64_t command_id)
      : commit_info(commit_info),
        command_id(command_id),
        vertex_edge{.action = Action::ADD_IN_EDGE,
                    .edge_type = edge_type,
                    .vertex = TaggedVertexPtr(vertex, sequencing == DeltaSequencing::NON_SEQUENTIAL
                                                          ? DeltaChainState::NON_SEQUENTIAL
                                                          : DeltaChainState::SEQUENTIAL),
                    .edge = edge} {}

  Delta(AddOutEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, CommitInfo *commit_info,
        uint64_t command_id)
      : Delta(AddOutEdgeTag{}, edge_type, vertex, edge, DeltaSequencing::SEQUENTIAL, commit_info, command_id) {}

  Delta(AddOutEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, DeltaSequencing sequencing,
        CommitInfo *commit_info, uint64_t command_id)
      : commit_info(commit_info),
        command_id(command_id),
        vertex_edge{.action = Action::ADD_OUT_EDGE,
                    .edge_type = edge_type,
                    .vertex = TaggedVertexPtr(vertex, sequencing == DeltaSequencing::NON_SEQUENTIAL
                                                          ? DeltaChainState::NON_SEQUENTIAL
                                                          : DeltaChainState::SEQUENTIAL),
                    .edge = edge} {}

  Delta(RemoveInEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, CommitInfo *commit_info,
        uint64_t command_id)
      : Delta(RemoveInEdgeTag{}, edge_type, vertex, edge, DeltaChainState::SEQUENTIAL, commit_info, command_id) {}

  Delta(RemoveInEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, DeltaChainState state,
        CommitInfo *commit_info, uint64_t command_id)
      : commit_info(commit_info),
        command_id(command_id),
        vertex_edge{.action = Action::REMOVE_IN_EDGE,
                    .edge_type = edge_type,
                    .vertex = TaggedVertexPtr(vertex, state),
                    .edge = edge} {}

  Delta(RemoveOutEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, CommitInfo *commit_info,
        uint64_t command_id)
      : Delta(RemoveOutEdgeTag{}, edge_type, vertex, edge, DeltaChainState::SEQUENTIAL, commit_info, command_id) {}

  Delta(RemoveOutEdgeTag /*tag*/, EdgeTypeId edge_type, Vertex *vertex, EdgeRef edge, DeltaChainState state,
        CommitInfo *commit_info, uint64_t command_id)
      : commit_info(commit_info),
        command_id(command_id),
        vertex_edge{.action = Action::REMOVE_OUT_EDGE,
                    .edge_type = edge_type,
                    .vertex = TaggedVertexPtr(vertex, state),
                    .edge = edge} {}

  Delta(const Delta &) = delete;
  Delta(Delta &&) = delete;
  Delta &operator=(const Delta &) = delete;
  Delta &operator=(Delta &&) = delete;

  ~Delta() = default;

  // TODO: optimize with in-place copy
  CommitInfo *commit_info;
  uint64_t command_id;
  PreviousPtr prev;
  std::atomic<Delta *> next{nullptr};

  union {
    Action action;

    struct {
      Action action = Action::DELETE_DESERIALIZED_OBJECT;
      opt_str value;
    } old_disk_key;

    struct {
      Action action;
      LabelId value;
    } label;

    struct {
      Action action;
      PropertyId key;
      storage::pmr::PropertyValue *value = nullptr;
      Vertex *out_vertex{nullptr};  // Used by edge's delta to easily rebuild the edge
    } property;

    struct {
      Action action;
      EdgeTypeId edge_type;
      TaggedVertexPtr vertex;
      EdgeRef edge;
    } vertex_edge;
  };
};

constexpr bool CanBeNonSequential(Delta::Action action) noexcept {
  return action == Delta::Action::REMOVE_IN_EDGE || action == Delta::Action::REMOVE_OUT_EDGE;
}

inline bool IsDeltaNonSequential(Delta const &delta) {
  return CanBeNonSequential(delta.action) && delta.vertex_edge.vertex.GetState() == DeltaChainState::NON_SEQUENTIAL;
}

// This is important, we want fast discard of unlinked deltas,
static_assert(std::is_trivially_destructible_v<Delta>,
              "any allocations use PageSlabMemoryResource, lifetime linked to that, dtr should be trivial");

static_assert(alignof(Delta) >= 8, "The Delta should be aligned to at least 8!");

static_assert(sizeof(Delta) <= 56, "Delta size is at most 56 bytes");

}  // namespace memgraph::storage
