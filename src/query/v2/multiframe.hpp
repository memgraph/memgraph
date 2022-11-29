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

class ValidFramesReader;
class ValidFramesInvalidator;
class ItOnNonConstInvalidFrames;

class MultiFrame {
 public:
  friend class ValidFramesReader;
  friend class ValidFramesInvalidator;
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
  ValidFramesReader GetValidFramesReader();

  /*!
  Returns a object on which one can iterate in a for-loop. By doing so, you will only get frames that are in a valid
  state in the multiframe.
  Iteration goes in a deterministic order.
  One can modify the validity of the frame with this implementation.
  If you do not plan to modify the validity of the frames, use GetValidFramesReader instead as this is faster.
  */
  ValidFramesInvalidator GetValidFramesInvalidator();

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

class ValidFramesReader {
 public:
  ValidFramesReader(MultiFrame &multiframe);

  ~ValidFramesReader();
  ValidFramesReader(const ValidFramesReader &other) = delete;                 // copy constructor
  ValidFramesReader(ValidFramesReader &&other) noexcept = delete;             // move constructor
  ValidFramesReader &operator=(const ValidFramesReader &other) = delete;      // copy assignment
  ValidFramesReader &operator=(ValidFramesReader &&other) noexcept = delete;  // move assignment

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = Frame;
    using pointer = value_type *;
    using reference = Frame &;
    using internal_ptr = FrameWithValidity *;

    Iterator(internal_ptr ptr, ValidFramesReader &iterator_wrapper) : ptr_(ptr), iterator_wrapper_(iterator_wrapper) {}

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
    ValidFramesReader &iterator_wrapper_;
  };

  Iterator begin();
  Iterator end();

 private:
  MultiFrame &multiframe_;
};

class ValidFramesInvalidator {
 public:
  ValidFramesInvalidator(MultiFrame &multiframe);

  ~ValidFramesInvalidator();
  ValidFramesInvalidator(const ValidFramesInvalidator &other) = delete;                 // copy constructor
  ValidFramesInvalidator(ValidFramesInvalidator &&other) noexcept = delete;             // move constructor
  ValidFramesInvalidator &operator=(const ValidFramesInvalidator &other) = delete;      // copy assignment
  ValidFramesInvalidator &operator=(ValidFramesInvalidator &&other) noexcept = delete;  // move assignment

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = FrameWithValidity;
    using pointer = value_type *;
    using reference = FrameWithValidity &;
    using internal_ptr = FrameWithValidity *;

    Iterator(internal_ptr ptr, ValidFramesInvalidator &iterator_wrapper)
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
    ValidFramesInvalidator &iterator_wrapper_;
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
