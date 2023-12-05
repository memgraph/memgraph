// Copyright 2023 Memgraph Ltd.
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

#include <algorithm>
#include <cmath>
#include <cstdint>
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
    memory_->Deallocate(b, alloc_size, alignment);
    b = next;
  }
  current_buffer_ = nullptr;
  next_buffer_size_ = initial_size_;
  allocated_ = 0U;
}

void *MonotonicBufferResource::DoAllocate(size_t bytes, size_t alignment) {
  static_assert(std::is_same_v<size_t, uintptr_t>);
  static_assert(std::is_same_v<size_t, uint64_t>);
  auto push_current_buffer = [this, bytes, alignment](size_t next_size) {
    // Set size so that the bytes fit.
    const size_t size = next_size > bytes ? next_size : bytes;
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
    const auto maybe_bytes_for_buffer = RoundUint64ToMultiple(sizeof(Buffer), alloc_align);
    if (!maybe_bytes_for_buffer) throw BadAlloc("Allocation size overflow");
    const size_t bytes_for_buffer = *maybe_bytes_for_buffer;
    const size_t alloc_size = bytes_for_buffer + size;
    if (alloc_size < size) throw BadAlloc("Allocation size overflow");
    void *ptr = memory_->Allocate(alloc_size, alloc_align);
    // Instantiate the Buffer at the start of the allocated block.
    current_buffer_ = new (ptr) Buffer{current_buffer_, alloc_size - bytes_for_buffer, alloc_align};
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

// PoolResource
//
// Implementation is partially based on "Small Object Allocation" implementation
// from "Modern C++ Design" by Andrei Alexandrescu. While some other parts are
// based on `libstdc++-9.1` implementation.

namespace impl {

Pool::Pool(size_t block_size, unsigned char blocks_per_chunk, MemoryResource *memory)
    : blocks_per_chunk_(blocks_per_chunk), block_size_(block_size), chunks_(memory) {}

Pool::~Pool() { MG_ASSERT(chunks_.empty(), "You need to call Release before destruction!"); }

void *Pool::Allocate() {
  auto allocate_block_from_chunk = [this](Chunk *chunk) {
    unsigned char *available_block = chunk->data + (chunk->first_available_block_ix * block_size_);
    // Update free-list pointer (index in our case) by reading "next" from the
    // available_block.
    chunk->first_available_block_ix = *available_block;
    --chunk->blocks_available;
    return available_block;
  };
  if (last_alloc_chunk_ && last_alloc_chunk_->blocks_available > 0U)
    return allocate_block_from_chunk(last_alloc_chunk_);
  // Find a Chunk with available memory.
  for (auto &chunk : chunks_) {
    if (chunk.blocks_available > 0U) {
      last_alloc_chunk_ = &chunk;
      return allocate_block_from_chunk(last_alloc_chunk_);
    }
  }
  // We haven't found a Chunk with available memory, so allocate a new one.
  if (block_size_ > std::numeric_limits<size_t>::max() / blocks_per_chunk_) throw BadAlloc("Allocation size overflow");
  size_t data_size = blocks_per_chunk_ * block_size_;
  // Use the next pow2 of block_size_ as alignment, so that we cover alignment
  // requests between 1 and block_size_. Users of this class should make sure
  // that requested alignment of particular blocks is never greater than the
  // block itself.
  size_t alignment = Ceil2(block_size_);
  if (alignment < block_size_) throw BadAlloc("Allocation alignment overflow");
  auto *data = reinterpret_cast<unsigned char *>(GetUpstreamResource()->Allocate(data_size, alignment));
  // Form a free-list of blocks in data.
  for (unsigned char i = 0U; i < blocks_per_chunk_; ++i) {
    *(data + (i * block_size_)) = i + 1U;
  }
  Chunk chunk{data, 0, blocks_per_chunk_};
  // Insert the big block in the sorted position.
  auto it = std::lower_bound(chunks_.begin(), chunks_.end(), chunk,
                             [](const auto &a, const auto &b) { return a.data < b.data; });
  try {
    it = chunks_.insert(it, chunk);
  } catch (...) {
    GetUpstreamResource()->Deallocate(data, data_size, alignment);
    throw;
  }

  last_alloc_chunk_ = &*it;
  last_dealloc_chunk_ = &*it;
  return allocate_block_from_chunk(last_alloc_chunk_);
}

void Pool::Deallocate(void *p) {
  MG_ASSERT(last_dealloc_chunk_, "No chunk to deallocate");
  MG_ASSERT(!chunks_.empty(),
            "Expected a call to Deallocate after at least a "
            "single Allocate has been done.");
  auto is_in_chunk = [this, p](const Chunk &chunk) {
    auto ptr = reinterpret_cast<uintptr_t>(p);
    size_t data_size = blocks_per_chunk_ * block_size_;
    return reinterpret_cast<uintptr_t>(chunk.data) <= ptr && ptr < reinterpret_cast<uintptr_t>(chunk.data + data_size);
  };
  auto deallocate_block_from_chunk = [this, p](Chunk *chunk) {
    // NOTE: This check is not enough to cover all double-free issues.
    MG_ASSERT(chunk->blocks_available < blocks_per_chunk_,
              "Deallocating more blocks than a chunk can contain, possibly a "
              "double-free situation or we have a bug in the allocator.");
    // Link the block into the free-list
    auto *block = reinterpret_cast<unsigned char *>(p);
    *block = chunk->first_available_block_ix;
    chunk->first_available_block_ix = (block - chunk->data) / block_size_;
    chunk->blocks_available++;
  };
  if (is_in_chunk(*last_dealloc_chunk_)) {
    deallocate_block_from_chunk(last_dealloc_chunk_);
    return;
  }

  // Find the chunk which served this allocation
  Chunk chunk{reinterpret_cast<unsigned char *>(p) - blocks_per_chunk_ * block_size_, 0, 0};
  auto it = std::lower_bound(chunks_.begin(), chunks_.end(), chunk,
                             [](const auto &a, const auto &b) { return a.data <= b.data; });
  MG_ASSERT(it != chunks_.end(), "Failed deallocation in utils::Pool");
  MG_ASSERT(is_in_chunk(*it), "Failed deallocation in utils::Pool");

  // Update last_alloc_chunk_ as well because it now has a free block.
  // Additionally this corresponds with C++ pattern of allocations and
  // deallocations being done in reverse order.
  last_alloc_chunk_ = &*it;
  last_dealloc_chunk_ = &*it;
  deallocate_block_from_chunk(last_dealloc_chunk_);
  // TODO: We could release the Chunk to upstream memory
}

void Pool::Release() {
  for (auto &chunk : chunks_) {
    size_t data_size = blocks_per_chunk_ * block_size_;
    size_t alignment = Ceil2(block_size_);
    GetUpstreamResource()->Deallocate(chunk.data, data_size, alignment);
  }
  chunks_.clear();
  last_alloc_chunk_ = nullptr;
  last_dealloc_chunk_ = nullptr;
}

}  // namespace impl

PoolResource::PoolResource(size_t max_blocks_per_chunk, size_t max_block_size, MemoryResource *memory_pools,
                           MemoryResource *memory_unpooled)
    : pools_(memory_pools),
      unpooled_(memory_unpooled),
      max_blocks_per_chunk_(std::min(max_blocks_per_chunk, static_cast<size_t>(impl::Pool::MaxBlocksInChunk()))),
      max_block_size_(max_block_size) {
  MG_ASSERT(max_blocks_per_chunk_ > 0U, "Invalid number of blocks per chunk");
  MG_ASSERT(max_block_size_ > 0U, "Invalid size of block");
}

void *PoolResource::DoAllocate(size_t bytes, size_t alignment) {
  // Take the max of `bytes` and `alignment` so that we simplify handling
  // alignment requests.
  size_t block_size = std::max(bytes, alignment);
  // Check that we have received a regular allocation request with non-padded
  // structs/classes in play. These will always have
  // `sizeof(T) % alignof(T) == 0`. Special requests which don't have that
  // property can never be correctly handled with contiguous blocks. We would
  // have to write a general-purpose allocator which has to behave as complex
  // as malloc/free.
  if (block_size % alignment != 0) throw BadAlloc("Requested bytes must be a multiple of alignment");
  if (block_size > max_block_size_) {
    // Allocate a big block.
    BigBlock big_block{bytes, alignment, GetUpstreamResourceBlocks()->Allocate(bytes, alignment)};
    // Insert the big block in the sorted position.
    auto it = std::lower_bound(unpooled_.begin(), unpooled_.end(), big_block,
                               [](const auto &a, const auto &b) { return a.data < b.data; });
    try {
      unpooled_.insert(it, big_block);
    } catch (...) {
      GetUpstreamResourceBlocks()->Deallocate(big_block.data, bytes, alignment);
      throw;
    }
    return big_block.data;
  }
  // Allocate a regular block, first check if last_alloc_pool_ is suitable.
  if (last_alloc_pool_ && last_alloc_pool_->GetBlockSize() == block_size) {
    return last_alloc_pool_->Allocate();
  }
  // Find the pool with greater or equal block_size.
  impl::Pool pool(block_size, max_blocks_per_chunk_, GetUpstreamResource());
  auto it = std::lower_bound(pools_.begin(), pools_.end(), pool,
                             [](const auto &a, const auto &b) { return a.GetBlockSize() < b.GetBlockSize(); });
  if (it != pools_.end() && it->GetBlockSize() == block_size) {
    last_alloc_pool_ = &*it;
    last_dealloc_pool_ = &*it;
    return it->Allocate();
  }
  // We don't have a pool for this block_size, so insert it in the sorted
  // position.
  it = pools_.emplace(it, std::move(pool));
  last_alloc_pool_ = &*it;
  last_dealloc_pool_ = &*it;
  return it->Allocate();
}

void PoolResource::DoDeallocate(void *p, size_t bytes, size_t alignment) {
  size_t block_size = std::max(bytes, alignment);
  MG_ASSERT(block_size % alignment == 0,
            "PoolResource shouldn't serve allocation requests where bytes aren't "
            "a multiple of alignment");
  if (block_size > max_block_size_) {
    // Deallocate a big block.
    BigBlock big_block{bytes, alignment, p};
    auto it = std::lower_bound(unpooled_.begin(), unpooled_.end(), big_block,
                               [](const auto &a, const auto &b) { return a.data < b.data; });
    MG_ASSERT(it != unpooled_.end(), "Failed deallocation");
    MG_ASSERT(it->data == p && it->bytes == bytes && it->alignment == alignment, "Failed deallocation");
    unpooled_.erase(it);
    GetUpstreamResourceBlocks()->Deallocate(p, bytes, alignment);
    return;
  }
  // Deallocate a regular block, first check if last_dealloc_pool_ is suitable.
  if (last_dealloc_pool_ && last_dealloc_pool_->GetBlockSize() == block_size) return last_dealloc_pool_->Deallocate(p);
  // Find the pool with equal block_size.
  impl::Pool pool(block_size, max_blocks_per_chunk_, GetUpstreamResource());
  auto it = std::lower_bound(pools_.begin(), pools_.end(), pool,
                             [](const auto &a, const auto &b) { return a.GetBlockSize() < b.GetBlockSize(); });
  MG_ASSERT(it != pools_.end(), "Failed deallocation");
  MG_ASSERT(it->GetBlockSize() == block_size, "Failed deallocation");
  last_alloc_pool_ = &*it;
  last_dealloc_pool_ = &*it;
  return it->Deallocate(p);
}

void PoolResource::Release() {
  for (auto &pool : pools_) pool.Release();
  pools_.clear();
  for (auto &big_block : unpooled_)
    GetUpstreamResourceBlocks()->Deallocate(big_block.data, big_block.bytes, big_block.alignment);
  unpooled_.clear();
  last_alloc_pool_ = nullptr;
  last_dealloc_pool_ = nullptr;
}

// PoolResource END

}  // namespace memgraph::utils
