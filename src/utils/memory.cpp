#include "utils/memory.hpp"

#include <cmath>
#include <cstdint>
#include <limits>
#include <new>
#include <type_traits>

namespace utils {

// MonotonicBufferResource

namespace {

// NOTE: std::bad_alloc has no constructor accepting a message, so we wrap our
// exceptions in this class.
class BadAlloc final : public std::bad_alloc {
  std::string msg_;

 public:
  explicit BadAlloc(const std::string &msg) : msg_(msg) {}

  const char *what() const noexcept override { return msg_.c_str(); }
};

size_t GrowMonotonicBuffer(size_t current_size, size_t max_size) {
  double next_size = current_size * 1.34;
  if (next_size >= static_cast<double>(max_size)) {
    // Overflow, clamp to max_size
    return max_size;
  }
  return std::ceil(next_size);
}

}  // namespace

MonotonicBufferResource::MonotonicBufferResource(size_t initial_size)
    : initial_size_(initial_size) {}

MonotonicBufferResource::MonotonicBufferResource(size_t initial_size,
                                                 MemoryResource *memory)
    : memory_(memory), initial_size_(initial_size) {}

MonotonicBufferResource::MonotonicBufferResource(
    MonotonicBufferResource &&other) noexcept
    : memory_(other.memory_),
      current_buffer_(other.current_buffer_),
      initial_size_(other.initial_size_),
      allocated_(other.allocated_) {
  other.current_buffer_ = nullptr;
}

MonotonicBufferResource &MonotonicBufferResource::operator=(
    MonotonicBufferResource &&other) noexcept {
  if (this == &other) return *this;
  Release();
  memory_ = other.memory_;
  current_buffer_ = other.current_buffer_;
  initial_size_ = other.initial_size_;
  allocated_ = other.allocated_;
  other.current_buffer_ = nullptr;
  other.allocated_ = 0U;
  return *this;
}

void MonotonicBufferResource::Release() {
  for (auto *b = current_buffer_; b;) {
    auto *next = b->next;
    b->~Buffer();
    memory_->Deallocate(b, sizeof(b) + b->capacity);
    b = next;
  }
  current_buffer_ = nullptr;
  allocated_ = 0U;
}

void *MonotonicBufferResource::DoAllocate(size_t bytes, size_t alignment) {
  static_assert(std::is_same_v<size_t, uintptr_t>);
  const bool is_pow2 = alignment != 0U && (alignment & (alignment - 1U)) == 0U;
  if (bytes == 0U || !is_pow2) throw BadAlloc("Invalid allocation request");
  if (alignment > alignof(std::max_align_t))
    alignment = alignof(std::max_align_t);

  auto push_current_buffer = [this, bytes](size_t next_size) {
    // Set capacity so that the bytes fit.
    size_t capacity = next_size > bytes ? next_size : bytes;
    // Handle the case when we need to align `Buffer::data` to a greater
    // `alignment`. We will simply always allocate with
    // alignof(std::max_align_t), and `sizeof(Buffer)` needs to be a multiple of
    // that to keep `data` correctly aligned.
    static_assert(sizeof(Buffer) % alignof(std::max_align_t) == 0);
    size_t alloc_size = sizeof(Buffer) + capacity;
    if (alloc_size <= capacity) throw BadAlloc("Allocation size overflow");
    void *ptr = memory_->Allocate(alloc_size);
    current_buffer_ = new (ptr) Buffer{current_buffer_, capacity};
    allocated_ = 0;
  };

  if (!current_buffer_) push_current_buffer(initial_size_);
  char *buffer_head = current_buffer_->data() + allocated_;
  void *aligned_ptr = buffer_head;
  size_t available = current_buffer_->capacity - allocated_;
  if (!std::align(alignment, bytes, aligned_ptr, available)) {
    // Not enough memory, so allocate a new block with aligned data.
    push_current_buffer(next_buffer_size_);
    aligned_ptr = buffer_head = current_buffer_->data();
    next_buffer_size_ = GrowMonotonicBuffer(
        next_buffer_size_, std::numeric_limits<size_t>::max() - sizeof(Buffer));
  }
  if (reinterpret_cast<char *>(aligned_ptr) < buffer_head)
    throw BadAlloc("Allocation alignment overflow");
  if (reinterpret_cast<char *>(aligned_ptr) + bytes <= aligned_ptr)
    throw BadAlloc("Allocation size overflow");
  allocated_ =
      reinterpret_cast<char *>(aligned_ptr) - current_buffer_->data() + bytes;
  return aligned_ptr;
}

// MonotonicBufferResource END

}  // namespace utils
