// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/memory.hpp"

#include <pthread.h>
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <type_traits>

#include "utils/logging.hpp"

namespace memgraph::utils {

// MonotonicBufferResource

namespace {

size_t GrowMonotonicBuffer(size_t current_size, size_t max_size) {
  double next_size = current_size * 1.34;
  if (next_size >= static_cast<double>(max_size)) {
    // Overflow, clamp to max_size
    return max_size;
  }
  return std::ceil(next_size);
}

__attribute__((no_sanitize("pointer-overflow"))) void CheckAllocationSizeOverflow(void *aligned_ptr, size_t bytes) {
  if (reinterpret_cast<char *>(aligned_ptr) + bytes <= aligned_ptr) throw BadAlloc("Allocation size overflow");
}
}  // namespace

MonotonicBufferResource::MonotonicBufferResource(size_t initial_size) : initial_size_(initial_size) {}

MonotonicBufferResource::MonotonicBufferResource(size_t initial_size, MemoryResource *memory)
    : memory_(memory), initial_size_(initial_size) {}

MonotonicBufferResource::MonotonicBufferResource(void *buffer, size_t buffer_size, MemoryResource *memory)
    : memory_(memory), initial_buffer_(buffer), initial_size_(buffer_size) {}

MonotonicBufferResource::MonotonicBufferResource(MonotonicBufferResource &&other) noexcept
    : memory_(other.memory_),
      current_buffer_(other.current_buffer_),
      initial_buffer_(other.initial_buffer_),
      initial_size_(other.initial_size_),
      next_buffer_size_(other.next_buffer_size_),
      allocated_(other.allocated_) {
  other.current_buffer_ = nullptr;
}

MonotonicBufferResource &MonotonicBufferResource::operator=(MonotonicBufferResource &&other) noexcept {
  if (this == &other) return *this;
  Release();
  memory_ = other.memory_;
  current_buffer_ = other.current_buffer_;
  initial_buffer_ = other.initial_buffer_;
  initial_size_ = other.initial_size_;
  next_buffer_size_ = other.next_buffer_size_;
  allocated_ = other.allocated_;
  other.current_buffer_ = nullptr;
  other.allocated_ = 0U;
  return *this;
}

void MonotonicBufferResource::Release() {
  for (auto *b = current_buffer_; b;) {
    auto *next = b->next;
    auto alloc_size = b->size();
    auto alignment = b->alignment;
    b->~Buffer();
    memory_->deallocate(b, alloc_size, alignment);
    b = next;
  }
  current_buffer_ = nullptr;
  next_buffer_size_ = initial_size_;
  allocated_ = 0U;
}

void *MonotonicBufferResource::do_allocate(size_t bytes, size_t alignment) {
  static_assert(std::is_same_v<size_t, uintptr_t>);
  static_assert(std::is_same_v<size_t, uint64_t>);
  auto push_current_buffer = [this, bytes, alignment](size_t next_size) {
    // Set size so that the bytes fit.
    const size_t size = std::max(next_size, bytes);
    // Simplify alignment by always using values greater or equal to max_align
    const size_t alloc_align = std::max(alignment, alignof(std::max_align_t));
    // Setup the Buffer area before `Buffer::data` such that `Buffer::data` is
    // correctly aligned. Since we request the initial memory to be aligned to
    // `alloc_align`, we can just allocate an additional multiple of
    // `alloc_align` of bytes such that the `Buffer` fits. `Buffer::data` will
    // then be aligned after this multiple of bytes.
    static_assert(IsPow2(alignof(Buffer)),
                  "Buffer should not be a packed struct in order to be placed "
                  "at the start of an allocation request");
    // TODO : use better function than RoundUint64ToMultiple because we know alloc_align is a power of 2
    const auto maybe_bytes_for_buffer = RoundUint64ToMultiple(sizeof(Buffer), alloc_align);
    if (!maybe_bytes_for_buffer) throw BadAlloc("Allocation size overflow");
    const size_t bytes_for_buffer = *maybe_bytes_for_buffer;
    // TODO : use better function than RoundUint64ToMultiple because we know alloc_align is a power of 2
    if (size > std::numeric_limits<uint64_t>::max() - bytes_for_buffer) {
      throw BadAlloc("Allocation size overflow");
    }
    const auto alloc_size = RoundUint64ToMultiple(bytes_for_buffer + size, alloc_align);
    if (!alloc_size) throw BadAlloc("Allocation size overflow");
    void *ptr = memory_->allocate(*alloc_size, alloc_align);
    // Instantiate the Buffer at the start of the allocated block.
    current_buffer_ = new (ptr) Buffer{current_buffer_, *alloc_size - bytes_for_buffer, alloc_align};
    allocated_ = 0;
  };

  char *data = nullptr;
  size_t data_capacity = 0U;
  if (current_buffer_) {
    data = current_buffer_->data();
    data_capacity = current_buffer_->capacity;
  } else if (initial_buffer_) {
    data = reinterpret_cast<char *>(initial_buffer_);
    data_capacity = initial_size_;
  } else {  // missing current_buffer_ and initial_buffer_
    push_current_buffer(initial_size_);
    data = current_buffer_->data();
    data_capacity = current_buffer_->capacity;
  }
  char *buffer_head = data + allocated_;
  void *aligned_ptr = buffer_head;
  size_t available = data_capacity - allocated_;
  if (!std::align(alignment, bytes, aligned_ptr, available)) {
    // Not enough memory, so allocate a new block with aligned data.
    push_current_buffer(next_buffer_size_);
    aligned_ptr = buffer_head = data = current_buffer_->data();
    next_buffer_size_ = GrowMonotonicBuffer(next_buffer_size_, std::numeric_limits<size_t>::max() - sizeof(Buffer));
  }
  if (reinterpret_cast<char *>(aligned_ptr) < buffer_head) throw BadAlloc("Allocation alignment overflow");
  CheckAllocationSizeOverflow(aligned_ptr, bytes);
  allocated_ = reinterpret_cast<char *>(aligned_ptr) - data + bytes;
  return aligned_ptr;
}

// MonotonicBufferResource END

// ThreadSafeMonotonicBufferResource implementation

/// Release all allocated memory back to the upstream resource
void ThreadSafeMonotonicBufferResource::Release() {
  // Reset all memory
  Block *current = head_.load(std::memory_order_acquire);
  while (current) {
    Block *next = current->next;
    memory_->deallocate(current, current->total_size(), alignof(Block));
    current = next;
  }
  head_.store(nullptr, std::memory_order_release);
}

void *ThreadSafeMonotonicBufferResource::do_allocate(size_t bytes, size_t alignment) {
  // Try lock-free allocation first
  const auto [block, ptr] = try_lock_free_allocation(bytes, alignment);
  if (ptr) {
    CheckAllocationSizeOverflow(ptr, bytes);
    return ptr;
  }

  // Fallback to blocking allocation
  return allocate_with_lock(block, bytes, alignment);
}

std::pair<ThreadSafeMonotonicBufferResource::Block *, void *>
ThreadSafeMonotonicBufferResource::try_lock_free_allocation(size_t bytes, size_t alignment) {
  // Get the head block
  auto *block = head_.load(std::memory_order_acquire);
  if (!block) {
    return {nullptr, nullptr};  // No blocks available
  }

  // Try to reserve space in the block
  // Since we can't know the exact current ptr, reserve for the worst case
  auto size_to_reserve = bytes + alignment - 1;
  if (size_to_reserve > block->capacity) {
    return {block, nullptr};
  }

  const auto old_size = block->size.fetch_add(size_to_reserve, std::memory_order_acq_rel);
  if (old_size + size_to_reserve > block->capacity) {
    // Failure: return nullptr
    // NOTE: Small optimization: don't pay the penalty of another atomic operation (fetch_sub), block is full (move on
    // to the next)
    // Adding a new block also alows us to not scan over the blocks and just rely on the head_
    return {block, nullptr};
  }
  // Success: return the aligned pointer
  void *aligned_void_ptr = block->begin() + old_size;
  if (!std::align(alignment, bytes, aligned_void_ptr, size_to_reserve)) {
    // Not enough space, return failure
    return {block, nullptr};
  }
  return {block, aligned_void_ptr};
}

void *ThreadSafeMonotonicBufferResource::allocate_with_lock(Block *last_block, size_t bytes, size_t alignment) {
  std::unique_lock<std::mutex> lock(alloc_mutex_);

  // Double-check that we still need a new block
  auto *current_block = head_.load(std::memory_order_acquire);
  while (current_block && current_block != last_block) {
    lock.unlock();
    // Try lock-free allocation one more time
    auto [block, ptr] = try_lock_free_allocation(bytes, alignment);
    if (ptr) {
      CheckAllocationSizeOverflow(ptr, bytes);
      return ptr;
    }
    last_block = block;
    lock.lock();  // Make sure to lock before loading head
    current_block = head_.load(std::memory_order_acquire);
  }

  // Allocate new block with enough space for alignment
  auto *const new_block = allocate_new_block(current_block, bytes + alignment - 1);
  DMG_ASSERT(new_block, "Failed to throw on new block allocation error");

  // Try to align the pointer
  void *aligned_ptr = new_block->begin();
  size_t available = new_block->capacity;
  if (!std::align(alignment, bytes, aligned_ptr, available)) {
    throw BadAlloc("Failed to align pointer in new block");
  }

  CheckAllocationSizeOverflow(aligned_ptr, bytes);

  // Update new size
  const size_t new_size = new_block->capacity - available + bytes;
  new_block->size.store(new_size, std::memory_order_release);

  // Update the head atomically
  head_.store(new_block, std::memory_order_release);
  return aligned_ptr;
}

ThreadSafeMonotonicBufferResource::Block *ThreadSafeMonotonicBufferResource::allocate_new_block(Block *current_head,
                                                                                                size_t min_size) {
  // Use the next_buffer_size_ for exponential growth, but ensure it's at least min_size
  const size_t block_size = std::max(next_buffer_size_, min_size);

  // Allocate memory for block header + data
  size_t total_size = sizeof(Block) + block_size;
  // Size must be a multiple of alignof(Block)
  if (const auto maybe_total_size = RoundUint64ToMultiple(total_size, alignof(Block))) {
    total_size = *maybe_total_size;
  } else {
    throw BadAlloc("Allocation size overflow");
  }
  void *new_buffer = memory_->allocate(total_size, alignof(Block));

  if (!new_buffer) {
    throw BadAlloc("Failed to allocate new block");
  }

  // Create new block at the beginning of the allocated memory
  auto *new_block = new (new_buffer) Block(current_head, 0, total_size - sizeof(Block));

  // Grow the next buffer size for future allocations
  next_buffer_size_ = GrowMonotonicBuffer(next_buffer_size_, std::numeric_limits<size_t>::max() - sizeof(Block));

  return new_block;
}

// PoolResource
//
// Implementation is partially based on "Small Object Allocation" implementation
// from "Modern C++ Design" by Andrei Alexandrescu. While some other parts are
// based on `libstdc++-9.1` implementation.

namespace impl {

Pool::Pool(size_t block_size, unsigned char blocks_per_chunk, MemoryResource *chunk_memory)
    : blocks_per_chunk_(blocks_per_chunk), block_size_(block_size), chunks_(chunk_memory) {
  // Use the next pow2 of block_size_ as alignment, so that we cover alignment
  // requests between 1 and block_size_. Users of this class should make sure
  // that requested alignment of particular blocks is never greater than the
  // block itself.
  if (block_size_ > std::numeric_limits<size_t>::max() / blocks_per_chunk_) throw BadAlloc("Allocation size overflow");
}

Pool::~Pool() {
  if (!chunks_.empty()) {
    auto *resource = GetUpstreamResource();
    auto const dataSize = blocks_per_chunk_ * block_size_;
    auto const alignment = Ceil2(block_size_);
    for (auto &chunk : chunks_) {
      resource->deallocate(chunk.raw_data, dataSize, alignment);
    }
    chunks_.clear();
  }
  free_list_ = nullptr;
}

void *Pool::Allocate() {
  if (!free_list_) [[unlikely]] {
    // need new chunk
    auto const data_size = blocks_per_chunk_ * block_size_;
    auto const alignment = Ceil2(block_size_);
    auto *resource = GetUpstreamResource();
    auto *data = reinterpret_cast<std::byte *>(resource->allocate(data_size, alignment));
    try {
      auto &new_chunk = chunks_.emplace_front(data);
      free_list_ = new_chunk.build_freelist(block_size_, blocks_per_chunk_);
    } catch (...) {
      resource->deallocate(data, data_size, alignment);
      throw;
    }
  }
  return std::exchange(free_list_, *reinterpret_cast<std::byte **>(free_list_));
}

void Pool::Deallocate(void *p) {
  *reinterpret_cast<std::byte **>(p) = std::exchange(free_list_, reinterpret_cast<std::byte *>(p));
}

ThreadSafePool::Node *&ThreadSafePool::thread_local_head() {
  Node **head_ptr = static_cast<Node **>(pthread_getspecific(thread_local_head_key_));
  if (!head_ptr) {
    // Allocate space for the Node* pointer
    // Use an allocator that just drops the memory (no need to destroy each pointer <- it just points to managed memory)
    head_ptr = static_cast<Node **>(block_memory_resource_.allocate(sizeof(Node *)));
    if (!head_ptr) {
      throw BadAlloc("Failed to allocate thread-local head pointer");
    }
    *head_ptr = nullptr;
    pthread_setspecific(thread_local_head_key_, reinterpret_cast<void *>(head_ptr));
  }
  return *head_ptr;
}

// carve a new block and link its nodes
ThreadSafePool::Node *ThreadSafePool::carve_block() {
  // Allocate a new chunk
  void *raw = chunk_memory_->allocate(chunk_size(), chunk_alignment());
  {
    // Push back to the global list
    const std::unique_lock lock(mtx_);
    chunks_.emplace_front(raw);
  }

  // Link the blocks in the chunk
  Node *prev = nullptr;
  for (std::size_t i = 0; i < blocks_per_chunk_; ++i) {
    Node *node = reinterpret_cast<Node *>(reinterpret_cast<char *>(raw) + (i * block_size_));
    node->next = prev;
    prev = node;
  }

  // Return the top node - caller will update their thread-local head
  return prev;
}

void *ThreadSafePool::pop_head() {
  Node *head = thread_local_head();
  if (head) {
    thread_local_head() = head->next;
    head->next = nullptr;
    return reinterpret_cast<void *>(head);
  }
  return nullptr;
}

void ThreadSafePool::push_head(void *p) {
  Node *node = static_cast<Node *>(p);
  node->next = thread_local_head();
  thread_local_head() = node;
}

ThreadSafePool::ThreadSafePool(std::size_t block_size, std::size_t blocks_per_chunks, MemoryResource *chunk_memory)
    : block_size_(block_size), blocks_per_chunk_(blocks_per_chunks), chunk_memory_(chunk_memory) {
  // Create instance-specific pthread key for thread-local storage
  if (pthread_key_create(&thread_local_head_key_, nullptr) != 0) {
    throw BadAlloc("Failed to create pthread key for thread-local storage");
  }
}

ThreadSafePool::~ThreadSafePool() {
  if (!chunks_.empty()) {
    for (auto &chunk : chunks_) {
      chunk_memory_->deallocate(chunk, chunk_size(), chunk_alignment());
    }
    chunks_.clear();
  }
  // Clean up the pthread key
  pthread_key_delete(thread_local_head_key_);
}

void *ThreadSafePool::Allocate() {
  if (thread_local_head()) {
    auto *head = thread_local_head();
    thread_local_head() = head->next;
    return head;
  }
  auto *head = carve_block();
  thread_local_head() = head->next;
  return head;
}

void ThreadSafePool::Deallocate(void *p) noexcept {
  try {
    Node *node = static_cast<Node *>(p);
    node->next = thread_local_head();
    thread_local_head() = node;
  } catch (...) {
    // If thread_local_head() throws, we can't do much in a noexcept function
    // This should not happen in normal operation
    std::terminate();
  }
}

}  // namespace impl

struct NullMemoryResourceImpl final : public MemoryResource {
  NullMemoryResourceImpl() = default;
  NullMemoryResourceImpl(NullMemoryResourceImpl const &) = default;
  NullMemoryResourceImpl &operator=(NullMemoryResourceImpl const &) = default;
  NullMemoryResourceImpl(NullMemoryResourceImpl &&) = default;
  NullMemoryResourceImpl &operator=(NullMemoryResourceImpl &&) = default;
  ~NullMemoryResourceImpl() override = default;

 private:
  void *do_allocate(size_t /*bytes*/, size_t /*alignment*/) override {
    throw BadAlloc{"NullMemoryResource doesn't allocate"};
  }
  void do_deallocate(void * /*p*/, size_t /*bytes*/, size_t /*alignment*/) override {
    throw BadAlloc{"NullMemoryResource doesn't deallocate"};
  }
  bool do_is_equal(MemoryResource const &other) const noexcept override {
    return dynamic_cast<NullMemoryResourceImpl const *>(&other) != nullptr;
  }
};

MemoryResource *NullMemoryResource() noexcept {
  static auto res = NullMemoryResourceImpl{};
  return &res;
}

namespace impl {

/// 1 bit sensitivity test
static_assert(bin_index<1>(9U) == 0);
static_assert(bin_index<1>(10U) == 0);
static_assert(bin_index<1>(11U) == 0);
static_assert(bin_index<1>(12U) == 0);
static_assert(bin_index<1>(13U) == 0);
static_assert(bin_index<1>(14U) == 0);
static_assert(bin_index<1>(15U) == 0);
static_assert(bin_index<1>(16U) == 0);

static_assert(bin_index<1>(17U) == 1);
static_assert(bin_index<1>(18U) == 1);
static_assert(bin_index<1>(19U) == 1);
static_assert(bin_index<1>(20U) == 1);
static_assert(bin_index<1>(21U) == 1);
static_assert(bin_index<1>(22U) == 1);
static_assert(bin_index<1>(23U) == 1);
static_assert(bin_index<1>(24U) == 1);
static_assert(bin_index<1>(25U) == 1);
static_assert(bin_index<1>(26U) == 1);
static_assert(bin_index<1>(27U) == 1);
static_assert(bin_index<1>(28U) == 1);
static_assert(bin_index<1>(29U) == 1);
static_assert(bin_index<1>(30U) == 1);
static_assert(bin_index<1>(31U) == 1);
static_assert(bin_index<1>(32U) == 1);

/// 2 bit sensitivity test

static_assert(bin_index<2>(9U) == 0);
static_assert(bin_index<2>(10U) == 0);
static_assert(bin_index<2>(11U) == 0);
static_assert(bin_index<2>(12U) == 0);

static_assert(bin_index<2>(13U) == 1);
static_assert(bin_index<2>(14U) == 1);
static_assert(bin_index<2>(15U) == 1);
static_assert(bin_index<2>(16U) == 1);

static_assert(bin_index<2>(17U) == 2);
static_assert(bin_index<2>(18U) == 2);
static_assert(bin_index<2>(19U) == 2);
static_assert(bin_index<2>(20U) == 2);
static_assert(bin_index<2>(21U) == 2);
static_assert(bin_index<2>(22U) == 2);
static_assert(bin_index<2>(23U) == 2);
static_assert(bin_index<2>(24U) == 2);

}  // namespace impl

// Explicit template instantiations for PoolResource with default template parameter
template class PoolResource<impl::Pool>;
template class PoolResource<impl::ThreadSafePool>;

template <typename P>
void *PoolResource<P>::do_allocate(size_t bytes, size_t alignment) {
  // Take the max of `bytes` and `alignment` so that we simplify handling
  // alignment requests.
  size_t block_size = std::max({bytes, alignment, 1UL});
  // Check that we have received a regular allocation request with non-padded
  // structs/classes in play. These will always have
  // `sizeof(T) % alignof(T) == 0`. Special requests which don't have that
  // property can never be correctly handled with contiguous blocks. We would
  // have to write a general-purpose allocator which has to behave as complex
  // as malloc/free.
  if (block_size % alignment != 0) throw BadAlloc("Requested bytes must be a multiple of alignment");

  if (block_size <= 64) {
    return mini_pools_[(block_size - 1UL) / 8UL].Allocate();
  }
  if (block_size <= 128) {
    return pools_3bit_.allocate(block_size);
  }
  if (block_size <= 512) {
    return pools_4bit_.allocate(block_size);
  }
  if (block_size <= 1024) {
    return pools_5bit_.allocate(block_size);
  }
  return unpooled_memory_->allocate(bytes, alignment);
}
template <typename P>
void PoolResource<P>::do_deallocate(void *p, size_t bytes, size_t alignment) {
  size_t block_size = std::max({bytes, alignment, 1UL});
  DMG_ASSERT(block_size % alignment == 0);

  if (block_size <= 64) {
    mini_pools_[(block_size - 1UL) / 8UL].Deallocate(p);
  } else if (block_size <= 128) {
    pools_3bit_.deallocate(p, block_size);
  } else if (block_size <= 512) {
    pools_4bit_.deallocate(p, block_size);
  } else if (block_size <= 1024) {
    pools_5bit_.deallocate(p, block_size);
  } else {
    unpooled_memory_->deallocate(p, bytes, alignment);
  }
}
template <typename P>
bool PoolResource<P>::do_is_equal(const std::pmr::memory_resource &other) const noexcept {
  return this == &other;
}
}  // namespace memgraph::utils
