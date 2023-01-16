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

#pragma once

#include <iterator>

#include "query/v2/bindings/frame.hpp"

namespace memgraph::query::v2 {
constexpr uint64_t kNumberOfFramesInMultiframe = 1000;  // TODO have it configurable

class ValidFramesConsumer;
class ValidFramesModifier;
class ValidFramesReader;
class InvalidFramesPopulator;

class MultiFrame {
 public:
  friend class ValidFramesConsumer;
  friend class ValidFramesModifier;
  friend class ValidFramesReader;
  friend class InvalidFramesPopulator;

  MultiFrame(size_t size_of_frame, size_t number_of_frames, utils::MemoryResource *execution_memory);
  ~MultiFrame() = default;

  MultiFrame(const MultiFrame &other);
  MultiFrame(MultiFrame &&other) noexcept;
  MultiFrame &operator=(const MultiFrame &other) = delete;
  MultiFrame &operator=(MultiFrame &&other) noexcept = delete;

  /*
   * Returns a object on which one can iterate in a for-loop. By doing so, you will only get Frames that are in a valid
   * state in the MultiFrame.
   * Iteration goes in a deterministic order.
   * One can't modify the validity of the Frame nor its content with this implementation.
   */
  ValidFramesReader GetValidFramesReader();

  /*
   * Returns a object on which one can iterate in a for-loop. By doing so, you will only get Frames that are in a valid
   * state in the MultiFrame.
   * Iteration goes in a deterministic order.
   * One can't modify the validity of the Frame with this implementation. One can modify its content.
   */
  ValidFramesModifier GetValidFramesModifier();

  /*
   * Returns a object on which one can iterate in a for-loop. By doing so, you will only get Frames that are in a valid
   * state in the MultiFrame.
   * Iteration goes in a deterministic order.
   * One can modify the validity of the Frame with this implementation.
   * If you do not plan to modify the validity of the Frames, use GetValidFramesReader/GetValidFramesModifer instead as
   * this is faster.
   */
  ValidFramesConsumer GetValidFramesConsumer();

  /*
   * Returns a object on which one can iterate in a for-loop. By doing so, you will only get Frames that are in an
   * invalid state in the MultiFrame. Iteration goes in a deterministic order. One can modify the validity of
   * the Frame with this implementation.
   */
  InvalidFramesPopulator GetInvalidFramesPopulator();

  /**
   * Return the first Frame of the MultiFrame. This is only meant to be used in very specific cases. Please consider
   * using the iterators instead.
   * The Frame can be valid or invalid.
   */
  FrameWithValidity &GetFirstFrame();

  void MakeAllFramesInvalid() noexcept;

  bool HasValidFrame() const noexcept;
  bool HasInvalidFrame() const noexcept;

  inline utils::MemoryResource *GetMemoryResource() { return frames_[0].GetMemoryResource(); }

 private:
  void DefragmentValidFrames() noexcept;

  utils::pmr::vector<FrameWithValidity> frames_;
};

class ValidFramesReader {
 public:
  explicit ValidFramesReader(MultiFrame &multiframe);

  ~ValidFramesReader() = default;
  ValidFramesReader(const ValidFramesReader &other) = delete;
  ValidFramesReader(ValidFramesReader &&other) noexcept = delete;
  ValidFramesReader &operator=(const ValidFramesReader &other) = delete;
  ValidFramesReader &operator=(ValidFramesReader &&other) noexcept = delete;

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = const Frame;
    using pointer = value_type *;
    using reference = const Frame &;

    Iterator() = default;
    explicit Iterator(FrameWithValidity *ptr) : ptr_(ptr) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    Iterator &operator++() {
      ptr_++;
      return *this;
    }

    // NOLINTNEXTLINE(cert-dcl21-cpp)
    Iterator operator++(int) {
      auto old = *this;
      ptr_++;
      return old;
    }

    friend bool operator==(const Iterator &lhs, const Iterator &rhs) { return lhs.ptr_ == rhs.ptr_; };
    friend bool operator!=(const Iterator &lhs, const Iterator &rhs) { return lhs.ptr_ != rhs.ptr_; };

   private:
    FrameWithValidity *ptr_{nullptr};
  };

  Iterator begin();
  Iterator end();

 private:
  FrameWithValidity *after_last_valid_frame_;
  MultiFrame *multiframe_;
};

class ValidFramesModifier {
 public:
  explicit ValidFramesModifier(MultiFrame &multiframe);

  ~ValidFramesModifier() = default;
  ValidFramesModifier(const ValidFramesModifier &other) = delete;
  ValidFramesModifier(ValidFramesModifier &&other) noexcept = delete;
  ValidFramesModifier &operator=(const ValidFramesModifier &other) = delete;
  ValidFramesModifier &operator=(ValidFramesModifier &&other) noexcept = delete;

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = Frame;
    using pointer = value_type *;
    using reference = Frame &;

    Iterator() = default;
    Iterator(FrameWithValidity *ptr, ValidFramesModifier &iterator_wrapper)
        : ptr_(ptr), iterator_wrapper_(&iterator_wrapper) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    // Prefix increment
    Iterator &operator++() {
      do {
        ptr_++;
      } while (*this != iterator_wrapper_->end() && !ptr_->IsValid());

      return *this;
    }

    // NOLINTNEXTLINE(cert-dcl21-cpp)
    Iterator operator++(int) {
      auto old = *this;
      ++*this;
      return old;
    }

    friend bool operator==(const Iterator &lhs, const Iterator &rhs) { return lhs.ptr_ == rhs.ptr_; };
    friend bool operator!=(const Iterator &lhs, const Iterator &rhs) { return lhs.ptr_ != rhs.ptr_; };

   private:
    FrameWithValidity *ptr_{nullptr};
    ValidFramesModifier *iterator_wrapper_{nullptr};
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame *multiframe_;
};

class ValidFramesConsumer {
 public:
  explicit ValidFramesConsumer(MultiFrame &multiframe);

  ~ValidFramesConsumer() noexcept;
  ValidFramesConsumer(const ValidFramesConsumer &other) = default;
  ValidFramesConsumer(ValidFramesConsumer &&other) noexcept = default;
  ValidFramesConsumer &operator=(const ValidFramesConsumer &other) = default;
  ValidFramesConsumer &operator=(ValidFramesConsumer &&other) noexcept = delete;

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = FrameWithValidity;
    using pointer = value_type *;
    using reference = FrameWithValidity &;

    Iterator() = default;
    Iterator(FrameWithValidity *ptr, ValidFramesConsumer &iterator_wrapper)
        : ptr_(ptr), iterator_wrapper_(&iterator_wrapper) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    Iterator &operator++() {
      do {
        ptr_++;
      } while (*this != iterator_wrapper_->end() && !ptr_->IsValid());

      return *this;
    }

    // NOLINTNEXTLINE(cert-dcl21-cpp)
    Iterator operator++(int) {
      auto old = *this;
      ++*this;
      return old;
    }

    friend bool operator==(const Iterator &lhs, const Iterator &rhs) { return lhs.ptr_ == rhs.ptr_; };
    friend bool operator!=(const Iterator &lhs, const Iterator &rhs) { return lhs.ptr_ != rhs.ptr_; };

   private:
    FrameWithValidity *ptr_{nullptr};
    ValidFramesConsumer *iterator_wrapper_{nullptr};
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame *multiframe_;
};

class InvalidFramesPopulator {
 public:
  explicit InvalidFramesPopulator(MultiFrame &multiframe);
  ~InvalidFramesPopulator() = default;

  InvalidFramesPopulator(const InvalidFramesPopulator &other) = delete;
  InvalidFramesPopulator(InvalidFramesPopulator &&other) noexcept = delete;
  InvalidFramesPopulator &operator=(const InvalidFramesPopulator &other) = delete;
  InvalidFramesPopulator &operator=(InvalidFramesPopulator &&other) noexcept = delete;

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = FrameWithValidity;
    using pointer = value_type *;
    using reference = FrameWithValidity &;

    Iterator() = default;
    explicit Iterator(FrameWithValidity *ptr) : ptr_(ptr) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    Iterator &operator++() {
      ptr_->MakeValid();
      ptr_++;
      return *this;
    }

    // NOLINTNEXTLINE(cert-dcl21-cpp)
    Iterator operator++(int) {
      auto old = *this;
      ++ptr_;
      return old;
    }

    friend bool operator==(const Iterator &lhs, const Iterator &rhs) { return lhs.ptr_ == rhs.ptr_; };
    friend bool operator!=(const Iterator &lhs, const Iterator &rhs) { return lhs.ptr_ != rhs.ptr_; };

   private:
    FrameWithValidity *ptr_{nullptr};
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame *multiframe_;
};

}  // namespace memgraph::query::v2
