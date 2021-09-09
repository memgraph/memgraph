#include "storage_test_utils.hpp"

size_t CountVertices(storage::Storage::Accessor &storage_accessor, storage::View view) {
  auto vertices = storage_accessor.Vertices(view);
  size_t count = 0U;
  for (auto it = vertices.begin(); it != vertices.end(); ++it, ++count)
    ;
  return count;
}