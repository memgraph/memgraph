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

#pragma once

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <random>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "slk/streams.hpp"
#include "utils/logging.hpp"

class BinaryData {
 public:
  BinaryData(const uint8_t *data, size_t const size) : data_(new uint8_t[size]), size_(size) {
    memcpy(data_.get(), data, size);
  }

  BinaryData(std::unique_ptr<uint8_t[]> data, size_t size) : data_(std::move(data)), size_(size) {}

  const uint8_t *data() const { return data_.get(); }
  size_t size() const { return size_; }

  bool operator==(const BinaryData &other) const {
    if (size_ != other.size_) return false;
    for (size_t i = 0; i < size_; ++i) {
      if (data_[i] != other.data_[i]) return false;
    }
    return true;
  }

 private:
  std::unique_ptr<uint8_t[]> data_;
  size_t size_;
};

inline BinaryData operator+(const BinaryData &a, const BinaryData &b) {
  std::unique_ptr<uint8_t[]> data(new uint8_t[a.size() + b.size()]);
  memcpy(data.get(), a.data(), a.size());
  memcpy(data.get() + a.size(), b.data(), b.size());
  return BinaryData(std::move(data), a.size() + b.size());
}

inline BinaryData GetRandomData(size_t const size) {
  std::mt19937 gen(std::random_device{}());
  std::uniform_int_distribution<uint8_t> dis(0, 255);
  std::unique_ptr<uint8_t[]> ret(new uint8_t[size]);
  auto data = ret.get();
  for (size_t i = 0; i < size; ++i) {
    data[i] = dis(gen);
  }
  return BinaryData(std::move(ret), size);
}

inline std::vector<BinaryData> BufferToBinaryData(const uint8_t *data, size_t size, std::vector<size_t> sizes) {
  std::vector<BinaryData> ret;
  ret.reserve(sizes.size());
  size_t pos = 0;
  for (size_t i = 0; i < sizes.size(); ++i) {
    EXPECT_GE(size, pos + sizes[i]);
    ret.emplace_back(data + pos, sizes[i]);
    pos += sizes[i];
  }
  return ret;
}

inline BinaryData SizeToBinaryData(memgraph::slk::SegmentSize size) {
  return BinaryData(reinterpret_cast<const uint8_t *>(&size), sizeof(memgraph::slk::SegmentSize));
}

namespace memgraph::slk {

/// Class used for SLK tests. It creates a `memgraph::slk::Builder` that can be written
/// to. After you have written the data to the builder, you can get a
/// `memgraph::slk::Reader` and try to decode the encoded data.
class Loopback {
 public:
  ~Loopback() {
    MG_ASSERT(builder_, "You haven't created a builder!");
    MG_ASSERT(reader_, "You haven't created a reader!");
    reader_->Finalize();
  }

  memgraph::slk::Builder *GetBuilder() {
    MG_ASSERT(!builder_, "You have already allocated a builder!");
    builder_ = std::make_unique<memgraph::slk::Builder>(
        [this](const uint8_t *data, size_t size, bool have_more) { Write(data, size, have_more); });
    return builder_.get();
  }

  memgraph::slk::Reader *GetReader() {
    MG_ASSERT(builder_, "You must first get a builder before getting a reader!");
    MG_ASSERT(!reader_, "You have already allocated a reader!");
    builder_->Finalize();
    auto ret = memgraph::slk::CheckStreamStatus(data_.data(), data_.size());
    MG_ASSERT(ret.status == memgraph::slk::StreamStatus::COMPLETE);
    MG_ASSERT(ret.stream_size == data_.size());
    size_ = ret.encoded_data_size;
    Dump();
    reader_ = std::make_unique<memgraph::slk::Reader>(data_.data(), data_.size());
    return reader_.get();
  }

  size_t size() const { return size_; }

 private:
  void Write(const uint8_t *data, size_t size, bool have_more) {
    for (size_t i = 0; i < size; ++i) {
      data_.push_back(data[i]);
    }
  }

  void Dump() {
    std::string dump;
    for (size_t i = 0; i < data_.size(); ++i) {
      dump += fmt::format("{:02x}", data_[i]);
      if (i != data_.size() - 1) {
        dump += " ";
      }
    }
    // This stores the encoded SLK stream into the test XML output. To get the
    // data you have to specify to the test (during runtime) that it should
    // create an XML output.
    ::testing::Test::RecordProperty("slk_stream", dump);
  }

  std::vector<uint8_t> data_;
  std::unique_ptr<memgraph::slk::Builder> builder_;
  std::unique_ptr<memgraph::slk::Reader> reader_;
  size_t size_{0};
};

}  // namespace memgraph::slk
