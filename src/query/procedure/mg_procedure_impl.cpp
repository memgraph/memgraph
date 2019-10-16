#include "query/procedure/mg_procedure_impl.hpp"

#include <cstddef>
#include <cstring>

#include <algorithm>
#include <type_traits>

#include "utils/math.hpp"

void *mgp_alloc(mgp_memory *memory, size_t size_in_bytes) {
  return mgp_aligned_alloc(memory, size_in_bytes, alignof(std::max_align_t));
}

void *mgp_aligned_alloc(mgp_memory *memory, const size_t size_in_bytes,
                        const size_t alignment) {
  if (size_in_bytes == 0U || !utils::IsPow2(alignment)) return nullptr;
  // Simplify alignment by always using values greater or equal to max_align.
  const size_t alloc_align = std::max(alignment, alignof(std::max_align_t));
  // Allocate space for header containing size & alignment info.
  const size_t header_size = sizeof(size_in_bytes) + sizeof(alloc_align);
  // We need to return the `data` pointer aligned to the requested alignment.
  // Since we request the initial memory to be aligned to `alloc_align`, we can
  // just allocate an additional multiple of `alloc_align` of bytes such that
  // the header fits. `data` will then be aligned after this multiple of bytes.
  static_assert(std::is_same_v<size_t, uint64_t>);
  const auto maybe_bytes_for_header =
      utils::RoundUint64ToMultiple(header_size, alloc_align);
  if (!maybe_bytes_for_header) return nullptr;
  const size_t bytes_for_header = *maybe_bytes_for_header;
  const size_t alloc_size = bytes_for_header + size_in_bytes;
  if (alloc_size < size_in_bytes) return nullptr;
  try {
    void *ptr = memory->impl->Allocate(alloc_size, alloc_align);
    char *data = reinterpret_cast<char *>(ptr) + bytes_for_header;
    std::memcpy(data - sizeof(size_in_bytes), &size_in_bytes,
                sizeof(size_in_bytes));
    std::memcpy(data - sizeof(size_in_bytes) - sizeof(alloc_align),
                &alloc_align, sizeof(alloc_align));
    return data;
  } catch (...) {
    return nullptr;
  }
}

void mgp_free(mgp_memory *memory, void *const p) {
  if (!p) return;
  char *const data = reinterpret_cast<char *>(p);
  // Read the header containing size & alignment info.
  size_t size_in_bytes;
  std::memcpy(&size_in_bytes, data - sizeof(size_in_bytes),
              sizeof(size_in_bytes));
  size_t alloc_align;
  std::memcpy(&alloc_align, data - sizeof(size_in_bytes) - sizeof(alloc_align),
              sizeof(alloc_align));
  // Reconstruct how many bytes we allocated on top of the original request.
  // We need not check allocation request overflow, since we did so already in
  // mgp_aligned_alloc.
  const size_t header_size = sizeof(size_in_bytes) + sizeof(alloc_align);
  const size_t bytes_for_header =
      *utils::RoundUint64ToMultiple(header_size, alloc_align);
  const size_t alloc_size = bytes_for_header + size_in_bytes;
  // Get the original ptr we allocated.
  void *const original_ptr = data - bytes_for_header;
  memory->impl->Deallocate(original_ptr, alloc_size, alloc_align);
}
