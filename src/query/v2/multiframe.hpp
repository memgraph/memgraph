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
constexpr unsigned long kNumberOfFramesInMultiframe = 1000;  // #NoCommit have it configurable

class ItOnConstValidFrames;
class ItOnNonConstValidFrames;
class ItOnNonConstInvalidFrames;

class MultiFrame {
 public:
  friend class ItOnConstValidFrames;
  friend class ItOnNonConstValidFrames;
  friend class ItOnNonConstInvalidFrames;

  MultiFrame(FrameWithValidity default_frame, size_t number_of_frames, utils::MemoryResource *execution_memory);
  ~MultiFrame();

  MultiFrame(const MultiFrame &other);      // copy constructor
  MultiFrame(MultiFrame &&other) noexcept;  // move constructor
  MultiFrame &operator=(const MultiFrame &other) = delete;
  MultiFrame &operator=(MultiFrame &&other) noexcept = delete;

  /*!
  Returns a object on which one can iterate in a for-loop. By doing so, you will only get frames that are in a valid
  state in the multiframe.
  Iteration goes in a deterministic order.
  One can't modify the validity of the frame with this implementation.
  */
  ItOnConstValidFrames GetItOnConstValidFrames();

  /*!
  Returns a object on which one can iterate in a for-loop. By doing so, you will only get frames that are in a valid
  state in the multiframe.
  Iteration goes in a deterministic order.
  One can modify the validity of the frame with this implementation.
  If you do not plan to modify the validity of the frames, use GetItOnConstValidFrames instead as this is faster.
  */
  ItOnNonConstValidFrames GetItOnNonConstValidFrames();

  /*!
  Returns a object on which one can iterate in a for-loop. By doing so, you will only get frames that are in an invalid
  state in the multiframe.
  Iteration goes in a deterministic order.
  One can modify the validity of the frame with this implementation.
  */
  ItOnNonConstInvalidFrames GetItOnNonConstInvalidFrames();

  void ResetAllFramesInvalid() noexcept;

  bool HasValidFrame() const noexcept;

  inline utils::MemoryResource *GetMemoryResource() { return frames_[0].GetMemoryResource(); }

 private:
  void DefragmentValidFrames() noexcept;

  FrameWithValidity default_frame_;
  utils::pmr::vector<FrameWithValidity> frames_ =
      utils::pmr::vector<FrameWithValidity>(0, FrameWithValidity{1}, utils::NewDeleteResource());
};

class ItOnConstValidFrames {
 public:
  ItOnConstValidFrames(MultiFrame &multiframe);

  ~ItOnConstValidFrames();
  ItOnConstValidFrames(const ItOnConstValidFrames &other) = delete;                 // copy constructor
  ItOnConstValidFrames(ItOnConstValidFrames &&other) noexcept = delete;             // move constructor
  ItOnConstValidFrames &operator=(const ItOnConstValidFrames &other) = delete;      // copy assignment
  ItOnConstValidFrames &operator=(ItOnConstValidFrames &&other) noexcept = delete;  // move assignment

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = Frame;
    using pointer = value_type *;
    using reference = Frame &;
    using internal_ptr = FrameWithValidity *;

    Iterator(internal_ptr ptr, ItOnConstValidFrames &iterator_wrapper)
        : ptr_(ptr), iterator_wrapper_(iterator_wrapper) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    // Prefix increment
    Iterator &operator++() {
      do {
        ptr_++;
      } while (!this->ptr_->IsValid() && *this != iterator_wrapper_.end());

      return *this;
    }

    friend bool operator==(const Iterator &a, const Iterator &b) { return a.ptr_ == b.ptr_; };
    friend bool operator!=(const Iterator &a, const Iterator &b) { return a.ptr_ != b.ptr_; };

   private:
    internal_ptr ptr_;
    ItOnConstValidFrames &iterator_wrapper_;
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame &multiframe_;
};

class ItOnNonConstValidFrames {
 public:
  ItOnNonConstValidFrames(MultiFrame &multiframe);

  ~ItOnNonConstValidFrames();
  ItOnNonConstValidFrames(const ItOnNonConstValidFrames &other) = delete;                 // copy constructor
  ItOnNonConstValidFrames(ItOnNonConstValidFrames &&other) noexcept = delete;             // move constructor
  ItOnNonConstValidFrames &operator=(const ItOnNonConstValidFrames &other) = delete;      // copy assignment
  ItOnNonConstValidFrames &operator=(ItOnNonConstValidFrames &&other) noexcept = delete;  // move assignment

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = FrameWithValidity;
    using pointer = value_type *;
    using reference = FrameWithValidity &;
    using internal_ptr = FrameWithValidity *;

    Iterator(internal_ptr ptr, ItOnNonConstValidFrames &iterator_wrapper)
        : ptr_(ptr), iterator_wrapper_(iterator_wrapper) {}

    reference operator*() const { return *ptr_; }
    pointer operator->() { return ptr_; }

    // Prefix increment
    Iterator &operator++() {
      do {
        ptr_++;
      } while (!this->ptr_->IsValid() && *this != iterator_wrapper_.end());

      return *this;
    }

    friend bool operator==(const Iterator &a, const Iterator &b) { return a.ptr_ == b.ptr_; };
    friend bool operator!=(const Iterator &a, const Iterator &b) { return a.ptr_ != b.ptr_; };

   private:
    internal_ptr ptr_;
    ItOnNonConstValidFrames &iterator_wrapper_;
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame &multiframe_;
};

class ItOnNonConstInvalidFrames {
 public:
  ItOnNonConstInvalidFrames(MultiFrame &multiframe);
  ~ItOnNonConstInvalidFrames();

  ItOnNonConstInvalidFrames(const ItOnNonConstInvalidFrames &other) = delete;                 // copy constructor
  ItOnNonConstInvalidFrames(ItOnNonConstInvalidFrames &&other) noexcept = delete;             // move constructor
  ItOnNonConstInvalidFrames &operator=(const ItOnNonConstInvalidFrames &other) = delete;      // copy assignment
  ItOnNonConstInvalidFrames &operator=(ItOnNonConstInvalidFrames &&other) noexcept = delete;  // move assignment

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = FrameWithValidity;
    using pointer = value_type *;
    using reference = FrameWithValidity &;
    using internal_ptr = FrameWithValidity *;

    Iterator(internal_ptr ptr) : ptr_(ptr) {}

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
    internal_ptr ptr_;
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame &multiframe_;
};

}  // namespace memgraph::query::v2
