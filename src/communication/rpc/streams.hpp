#pragma once

#include <cstdint>

#include <glog/logging.h>

namespace slk {

// TODO (mferencevic): Implementations of the `Builder` and `Reader` are just
// mock implementations for now. They will be finished when they will be
// integrated into the RPC layer.

class Builder {
 public:
  void Save(const uint8_t *data, uint64_t size) {
    CHECK(size_ + size <= 262144);
    memcpy(data_ + size_, data, size);
    size_ += size;
  }

  uint8_t *data() { return data_; }
  uint64_t size() { return size_; }

 private:
  uint8_t data_[262144];
  uint64_t size_{0};
};

class Reader {
 public:
  Reader(const uint8_t *data, uint64_t size) : data_(data), size_(size) {}

  void Load(uint8_t *data, uint64_t size) {
    CHECK(offset_ <= size_);
    memcpy(data, data_ + offset_, size);
    offset_ += size;
  }

 private:
  const uint8_t *data_;
  uint64_t size_;
  uint64_t offset_{0};
};

}  // namespace slk
