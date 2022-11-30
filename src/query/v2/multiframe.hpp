// Copyright 2022 Memgraph Ltd.
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

  MultiFrame(int64_t size_of_frame, size_t number_of_frames, utils::MemoryResource *execution_memory);
  ~MultiFrame() = default;

  MultiFrame(const MultiFrame &other);      // copy constructor
  MultiFrame(MultiFrame &&other) noexcept;  // move constructor
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

  void MakeAllFramesInvalid() noexcept;

  bool HasValidFrame() const noexcept;

  inline utils::MemoryResource *GetMemoryResource() { return frames_[0].GetMemoryResource(); }

 private:
  // NOLINTNEXTLINE (bugprone-exception-escape)
  void DefragmentValidFrames() noexcept;

  utils::pmr::vector<FrameWithValidity> frames_ =
      utils::pmr::vector<FrameWithValidity>(0, FrameWithValidity{1}, utils::NewDeleteResource());
};

class ValidFramesReader {
 public:
  explicit ValidFramesReader(MultiFrame &multiframe);

  ~ValidFramesReader() = default;
  ValidFramesReader(const ValidFramesReader &other) = delete;                 // copy constructor
  ValidFramesReader(ValidFramesReader &&other) noexcept = delete;             // move constructor
  ValidFramesReader &operator=(const ValidFramesReader &other) = delete;      // copy assignment
  ValidFramesReader &operator=(ValidFramesReader &&other) noexcept = delete;  // move assignment

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = const Frame;
    using pointer = value_type *;
    using reference = const Frame &;

    Iterator(FrameWithValidity *ptr, ValidFramesReader &iterator_wrapper)
        : ptr_(ptr), iterator_wrapper_(iterator_wrapper) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    // Prefix increment
    Iterator &operator++() {
      do {
        ptr_++;
      } while (*this != iterator_wrapper_.end() && !this->ptr_->IsValid());

      return *this;
    }

    friend bool operator==(const Iterator &a, const Iterator &b) { return a.ptr_ == b.ptr_; };
    friend bool operator!=(const Iterator &a, const Iterator &b) { return a.ptr_ != b.ptr_; };

   private:
    FrameWithValidity *ptr_;
    ValidFramesReader &iterator_wrapper_;
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame &multiframe_;
};

class ValidFramesModifier {
 public:
  explicit ValidFramesModifier(MultiFrame &multiframe);

  ~ValidFramesModifier() = default;
  ValidFramesModifier(const ValidFramesModifier &other) = delete;                 // copy constructor
  ValidFramesModifier(ValidFramesModifier &&other) noexcept = delete;             // move constructor
  ValidFramesModifier &operator=(const ValidFramesModifier &other) = delete;      // copy assignment
  ValidFramesModifier &operator=(ValidFramesModifier &&other) noexcept = delete;  // move assignment

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = Frame;
    using pointer = value_type *;
    using reference = Frame &;

    Iterator(FrameWithValidity *ptr, ValidFramesModifier &iterator_wrapper)
        : ptr_(ptr), iterator_wrapper_(iterator_wrapper) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    // Prefix increment
    Iterator &operator++() {
      do {
        ptr_++;
      } while (*this != iterator_wrapper_.end() && !this->ptr_->IsValid());

      return *this;
    }

    friend bool operator==(const Iterator &a, const Iterator &b) { return a.ptr_ == b.ptr_; };
    friend bool operator!=(const Iterator &a, const Iterator &b) { return a.ptr_ != b.ptr_; };

   private:
    FrameWithValidity *ptr_;
    ValidFramesModifier &iterator_wrapper_;
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame &multiframe_;
};

class ValidFramesConsumer {
 public:
  explicit ValidFramesConsumer(MultiFrame &multiframe);

  ~ValidFramesConsumer() noexcept;
  ValidFramesConsumer(const ValidFramesConsumer &other) = delete;                 // copy constructor
  ValidFramesConsumer(ValidFramesConsumer &&other) noexcept = delete;             // move constructor
  ValidFramesConsumer &operator=(const ValidFramesConsumer &other) = delete;      // copy assignment
  ValidFramesConsumer &operator=(ValidFramesConsumer &&other) noexcept = delete;  // move assignment

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = FrameWithValidity;
    using pointer = value_type *;
    using reference = FrameWithValidity &;

    Iterator(FrameWithValidity *ptr, ValidFramesConsumer &iterator_wrapper)
        : ptr_(ptr), iterator_wrapper_(iterator_wrapper) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    // Prefix increment
    Iterator &operator++() {
      do {
        ptr_++;
      } while (*this != iterator_wrapper_.end() && !this->ptr_->IsValid());

      return *this;
    }

    friend bool operator==(const Iterator &a, const Iterator &b) { return a.ptr_ == b.ptr_; };
    friend bool operator!=(const Iterator &a, const Iterator &b) { return a.ptr_ != b.ptr_; };

   private:
    FrameWithValidity *ptr_;
    ValidFramesConsumer &iterator_wrapper_;
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame &multiframe_;
};

class InvalidFramesPopulator {
 public:
  explicit InvalidFramesPopulator(MultiFrame &multiframe);
  ~InvalidFramesPopulator() = default;

  InvalidFramesPopulator(const InvalidFramesPopulator &other) = delete;                 // copy constructor
  InvalidFramesPopulator(InvalidFramesPopulator &&other) noexcept = delete;             // move constructor
  InvalidFramesPopulator &operator=(const InvalidFramesPopulator &other) = delete;      // copy assignment
  InvalidFramesPopulator &operator=(InvalidFramesPopulator &&other) noexcept = delete;  // move assignment

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = FrameWithValidity;
    using pointer = value_type *;
    using reference = FrameWithValidity &;

    explicit Iterator(FrameWithValidity *ptr) : ptr_(ptr) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    // Prefix increment
    Iterator &operator++() {
      ptr_->MakeValid();
      ptr_++;
      return *this;
    }

    friend bool operator==(const Iterator &a, const Iterator &b) { return a.ptr_ == b.ptr_; };
    friend bool operator!=(const Iterator &a, const Iterator &b) { return a.ptr_ != b.ptr_; };

   private:
    FrameWithValidity *ptr_;
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame &multiframe_;
};

}  // namespace memgraph::query::v2
