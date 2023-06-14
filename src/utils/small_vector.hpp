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

//===- small_vector.hpp - 'Normally small' vectors --------*- C++ -*-===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// This file defines the SmallVector class.
//
//===----------------------------------------------------------------------===//
#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <type_traits>

#include "utils/likely.hpp"

// TODO (dsantl) This is original definition of LLVM_NODISCARD:
/// LLVM_NODISCARD - Warn if a type or return value is discarded.
// #if __cplusplus > 201402L && __has_cpp_attribute(nodiscard)
// #define LLVM_NODISCARD [[nodiscard]]
// #elif !__cplusplus
// // Workaround for llvm.org/PR23435, since clang 3.6 and below emit a spurious
// // error when __has_cpp_attribute is given a scoped attribute in C mode.
// #define LLVM_NODISCARD
// #elif __has_cpp_attribute(clang::warn_unused_result)
// #define LLVM_NODISCARD [[clang::warn_unused_result]]
// #else
// #define LLVM_NODISCARD
// #endif
#define LLVM_NODISCARD

// LLVM External Functions
namespace memgraph::utils {
namespace detail {
/// NextPowerOf2 - Returns the next power of two (in 64-bits)
/// that is strictly greater than a.  Returns zero on overflow.
inline uint64_t NextPowerOf2(uint64_t a) {
  a |= (a >> 1);
  a |= (a >> 2);
  a |= (a >> 4);
  a |= (a >> 8);
  a |= (a >> 16);
  a |= (a >> 32);
  return a + 1;
}
}  // namespace detail

/// This is all the non-templated stuff common to all SmallVectors.
class SmallVectorBase {
 protected:
  void *begin_x_, *end_x_, *capacity_x_;

 protected:
  SmallVectorBase(void *first_el, size_t size)
      : begin_x_(first_el), end_x_(first_el), capacity_x_((char *)first_el + size) {}

  /// This is an implementation of the Grow() method which only works
  /// on POD-like data types and is out of line to reduce code duplication.
  void GrowPod(void *first_el, size_t min_size_in_bytes, size_t t_size);

 public:
  /// This returns size()*sizeof(T).
  size_t size_in_bytes() const { return size_t((char *)end_x_ - (char *)begin_x_); }

  /// capacity_in_bytes - This returns capacity()*sizeof(T).
  size_t capacity_in_bytes() const { return size_t((char *)capacity_x_ - (char *)begin_x_); }

  LLVM_NODISCARD bool empty() const { return begin_x_ == end_x_; }
};

template <typename T, unsigned N>
struct SmallVectorStorage;

/// This is the part of SmallVectorTemplateBase which does not depend on whether
/// the type T is a POD. The extra dummy template argument is used by ArrayRef
/// to avoid unnecessarily requiring T to be complete.
template <typename T, typename = void>
class SmallVectorTemplateCommon : public SmallVectorBase {
 private:
  template <typename, unsigned>
  friend struct SmallVectorStorage;

  // Allocate raw space for n elements of type T.  If T has a ctor or dtor, we
  // don't want it to be automatically run, so we need to represent the space as
  // something else.  Use an array of char of sufficient alignment.
  ////////////typedef utils::AlignedCharArrayUnion<T> U;
  typedef typename std::aligned_union<1, T>::type U;
  U first_el_;
  // Space after 'first_el' is clobbered, do not add any instance vars after it.

 protected:
  SmallVectorTemplateCommon(size_t size) : SmallVectorBase(&first_el_, size) {}

  void GrowPod(size_t min_size_in_bytes, size_t t_size) {
    SmallVectorBase::GrowPod(&first_el_, min_size_in_bytes, t_size);
  }

  /// Return true if this is a smallvector which has not had dynamic
  /// memory allocated for it.
  bool IsSmall() const { return begin_x_ == static_cast<const void *>(&first_el_); }

  /// Put this vector in a state of being small.
  void ResetToSmall() { begin_x_ = end_x_ = capacity_x_ = &first_el_; }

  void SetEnd(T *P) { this->end_x_ = P; }

 public:
  typedef size_t size_type;
  typedef ptrdiff_t difference_type;
  typedef T value_type;
  typedef T *iterator;
  typedef const T *const_iterator;

  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;
  typedef std::reverse_iterator<iterator> reverse_iterator;

  typedef T &reference;
  typedef const T &const_reference;
  typedef T *pointer;
  typedef const T *const_pointer;

  // forward iterator creation methods.
  inline iterator begin() { return (iterator)this->begin_x_; }
  inline const_iterator begin() const { return (const_iterator)this->begin_x_; }
  inline iterator end() { return (iterator)this->end_x_; }
  inline const_iterator end() const { return (const_iterator)this->end_x_; }

 protected:
  iterator CapacityPtr() { return (iterator)this->capacity_x_; }
  const_iterator CapacityPtr() const { return (const_iterator)this->capacity_x_; }

 public:
  // reverse iterator creation methods.
  reverse_iterator rbegin() { return reverse_iterator(end()); }
  const_reverse_iterator rbegin() const { return const_reverse_iterator(end()); }
  reverse_iterator rend() { return reverse_iterator(begin()); }
  const_reverse_iterator rend() const { return const_reverse_iterator(begin()); }

  inline size_type size() const { return end() - begin(); }
  size_type max_size() const { return size_type(-1) / sizeof(T); }

  /// Return the total number of elements in the currently allocated buffer.
  size_t capacity() const { return CapacityPtr() - begin(); }

  /// Return a pointer to the vector's buffer, even if empty().
  pointer data() { return pointer(begin()); }
  /// Return a pointer to the vector's buffer, even if empty().
  const_pointer data() const { return const_pointer(begin()); }

  inline reference operator[](size_type idx) {
    assert(idx < size());
    return begin()[idx];
  }
  inline const_reference operator[](size_type idx) const {
    assert(idx < size());
    return begin()[idx];
  }

  reference front() {
    assert(!empty());
    return begin()[0];
  }
  const_reference front() const {
    assert(!empty());
    return begin()[0];
  }

  reference back() {
    assert(!empty());
    return end()[-1];
  }
  const_reference back() const {
    assert(!empty());
    return end()[-1];
  }
};

/// SmallVectorTemplateBase<TIsPodLike = false> - This is where we put method
/// implementations that are designed to work with non-POD-like T's.
template <typename T, bool TIsPodLike>
class SmallVectorTemplateBase : public SmallVectorTemplateCommon<T> {
 protected:
  SmallVectorTemplateBase(size_t size) : SmallVectorTemplateCommon<T>(size) {}

  static void DestroyRange(T *s, T *e) {
    while (s != e) {
      --e;
      e->~T();
    }
  }

  /// Move the range [i, e) into the uninitialized memory starting with "dest",
  /// constructing elements as needed.
  template <typename TIt1, typename TIt2>
  static void UninitializedMove(TIt1 i, TIt1 e, TIt2 dest) {
    std::uninitialized_copy(std::make_move_iterator(i), std::make_move_iterator(e), dest);
  }

  /// Copy the range [i, e) onto the uninitialized memory starting with "dest",
  /// constructing elements as needed.
  template <typename TIt1, typename TIt2>
  static void UninitializedCopy(TIt1 i, TIt1 e, TIt2 dest) {
    std::uninitialized_copy(i, e, dest);
  }

  /// Grow the allocated memory (without initializing new elements), doubling
  /// the size of the allocated memory. Guarantees space for at least one more
  /// element, or min_size more elements if specified.
  void Grow(size_t min_size = 0);

 public:
  void push_back(const T &elt) {
    if (UNLIKELY(this->end_x_ >= this->capacity_x_)) this->Grow();
    ::new ((void *)this->end()) T(elt);
    this->SetEnd(this->end() + 1);
  }

  void push_back(T &&elt) {
    if (UNLIKELY(this->end_x_ >= this->capacity_x_)) this->Grow();
    ::new ((void *)this->end()) T(::std::move(elt));
    this->SetEnd(this->end() + 1);
  }

  void pop_back() {
    this->SetEnd(this->end() - 1);
    this->end()->~T();
  }
};

// Define this out-of-line to dissuade the C++ compiler from inlining it.
template <typename T, bool TIsPodLike>
void SmallVectorTemplateBase<T, TIsPodLike>::Grow(size_t min_size) {
  size_t cur_capacity = this->capacity();
  size_t cur_size = this->size();
  // Always Grow, even from zero.
  size_t new_capacity = size_t(utils::detail::NextPowerOf2(cur_capacity + 2));
  if (new_capacity < min_size) new_capacity = min_size;
  T *new_elts = static_cast<T *>(malloc(new_capacity * sizeof(T)));

  // Move the elements over.
  this->UninitializedMove(this->begin(), this->end(), new_elts);

  // Destroy the original elements.
  DestroyRange(this->begin(), this->end());

  // If this wasn't grown from the inline copy, deallocate the old space.
  if (!this->IsSmall()) free(this->begin());

  this->SetEnd(new_elts + cur_size);
  this->begin_x_ = new_elts;
  this->capacity_x_ = this->begin() + new_capacity;
}

/// SmallVectorTemplateBase<TIsPodLike = true> - This is where we put method
/// implementations that are designed to work with POD-like T's.
template <typename T>
class SmallVectorTemplateBase<T, true> : public SmallVectorTemplateCommon<T> {
 protected:
  SmallVectorTemplateBase(size_t size) : SmallVectorTemplateCommon<T>(size) {}

  // No need to do a destroy loop for POD's.
  static void DestroyRange(T *, T *) {}

  /// Move the range [i, e) onto the uninitialized memory
  /// starting with "dest", constructing elements into it as needed.
  template <typename TIt1, typename TIt2>
  static void UninitializedMove(TIt1 i, TIt1 e, TIt2 dest) {
    // Just do a copy.
    UninitializedCopy(i, e, dest);
  }

  /// Copy the range [i, e) onto the uninitialized memory
  /// starting with "dest", constructing elements into it as needed.
  template <typename TIt1, typename TIt2>
  static void UninitializedCopy(TIt1 i, TIt1 e, TIt2 dest) {
    // Arbitrary iterator types; just use the basic implementation.
    std::uninitialized_copy(i, e, dest);
  }

  /// Copy the range [i, e) onto the uninitialized memory
  /// starting with "dest", constructing elements into it as needed.
  template <typename T1, typename T2>
  requires std::is_same_v<std::remove_const_t<T1>, T2>
  static void UninitializedCopy(T1 *i, T1 *e, T2 *dest) {
    // Use memcpy for PODs iterated by pointers (which includes SmallVector
    // iterators): std::uninitialized_copy optimizes to memmove, but we can
    // use memcpy here. Note that i and e are iterators and thus might be
    // invalid for memcpy if they are equal.
    if (i != e) memcpy(dest, i, (e - i) * sizeof(T));
  }

  /// Double the size of the allocated memory, guaranteeing space for at
  /// least one more element or min_size if specified.
  void Grow(size_t min_size = 0) { this->GrowPod(min_size * sizeof(T), sizeof(T)); }

 public:
  void push_back(const T &elt) {
    if (UNLIKELY(this->end_x_ >= this->capacity_x_)) this->Grow();
    memcpy(this->end(), &elt, sizeof(T));
    this->SetEnd(this->end() + 1);
  }

  void pop_back() { this->SetEnd(this->end() - 1); }
};

template <typename T>
inline constexpr bool is_pod = std::is_standard_layout_v<T> &&std::is_trivial_v<T>;

/// This class consists of common code factored out of the SmallVector class to
/// reduce code duplication based on the SmallVector 'n' template parameter.
template <typename T>
class SmallVectorImpl : public SmallVectorTemplateBase<T, is_pod<T>> {
  typedef SmallVectorTemplateBase<T, is_pod<T>> SuperClass;

  SmallVectorImpl(const SmallVectorImpl &) = delete;

 public:
  typedef typename SuperClass::iterator iterator;
  typedef typename SuperClass::const_iterator const_iterator;
  typedef typename SuperClass::size_type size_type;

 protected:
  // Default ctor - Initialize to empty.
  explicit SmallVectorImpl(unsigned n) : SmallVectorTemplateBase<T, is_pod<T>>(n * sizeof(T)) {}

 public:
  ~SmallVectorImpl() {
    // Destroy the constructed elements in the vector.
    this->DestroyRange(this->begin(), this->end());

    // If this wasn't grown from the inline copy, deallocate the old space.
    if (!this->IsSmall()) free(this->begin());
  }

  void clear() {
    this->DestroyRange(this->begin(), this->end());
    this->end_x_ = this->begin_x_;
  }

  void resize(size_type n) {
    if (n < this->size()) {
      this->DestroyRange(this->begin() + n, this->end());
      this->SetEnd(this->begin() + n);
    } else if (n > this->size()) {
      if (this->capacity() < n) this->Grow(n);
      for (auto i = this->end(), e = this->begin() + n; i != e; ++i) new (&*i) T();
      this->SetEnd(this->begin() + n);
    }
  }

  void resize(size_type n, const T &nv) {
    if (n < this->size()) {
      this->DestroyRange(this->begin() + n, this->end());
      this->SetEnd(this->begin() + n);
    } else if (n > this->size()) {
      if (this->capacity() < n) this->Grow(n);
      std::uninitialized_fill(this->end(), this->begin() + n, nv);
      this->SetEnd(this->begin() + n);
    }
  }

  void reserve(size_type n) {
    if (this->capacity() < n) this->Grow(n);
  }

  LLVM_NODISCARD T pop_back_val() {
    T result = ::std::move(this->back());
    this->pop_back();
    return result;
  }

  void swap(SmallVectorImpl &rhs);

  /// Add the specified range to the end of the SmallVector.
  template <typename TInIter>
  void append(TInIter in_start, TInIter in_end) {
    size_type num_inputs = std::distance(in_start, in_end);
    // Grow allocated space if needed.
    if (num_inputs > size_type(this->CapacityPtr() - this->end())) this->Grow(this->size() + num_inputs);

    // Copy the new elements over.
    this->UninitializedCopy(in_start, in_end, this->end());
    this->SetEnd(this->end() + num_inputs);
  }

  /// Add the specified range to the end of the SmallVector.
  void append(size_type num_inputs, const T &elt) {
    // Grow allocated space if needed.
    if (num_inputs > size_type(this->CapacityPtr() - this->end())) this->Grow(this->size() + num_inputs);

    // Copy the new elements over.
    std::uninitialized_fill_n(this->end(), num_inputs, elt);
    this->SetEnd(this->end() + num_inputs);
  }

  void append(std::initializer_list<T> il) { append(il.begin(), il.end()); }

  void assign(size_type num_elts, const T &elt) {
    clear();
    if (this->capacity() < num_elts) this->Grow(num_elts);
    this->SetEnd(this->begin() + num_elts);
    std::uninitialized_fill(this->begin(), this->end(), elt);
  }

  void assign(std::initializer_list<T> il) {
    clear();
    append(il);
  }

  iterator erase(const_iterator ci) {
    // Just cast away constness because this is a non-const member function.
    iterator i = const_cast<iterator>(ci);

    assert(i >= this->begin() && "Iterator to erase is out of bounds.");
    assert(i < this->end() && "Erasing at past-the-end iterator.");

    iterator n = i;
    // Shift all elts down one.
    std::move(i + 1, this->end(), i);
    // Drop the last elt.
    this->pop_back();
    return (n);
  }

  iterator erase(const_iterator cs, const_iterator ce) {
    // Just cast away constness because this is a non-const member function.
    iterator s = const_cast<iterator>(cs);
    iterator e = const_cast<iterator>(ce);

    assert(s >= this->begin() && "Range to erase is out of bounds.");
    assert(s <= e && "Trying to erase invalid range.");
    assert(e <= this->end() && "Trying to erase past the end.");

    iterator n = s;
    // Shift all elts down.
    iterator i = std::move(e, this->end(), s);
    // Drop the last elts.
    this->DestroyRange(i, this->end());
    this->SetEnd(i);
    return (n);
  }

  iterator insert(iterator i, T &&elt) {
    if (i == this->end()) {  // Important special case for empty vector.
      this->push_back(::std::move(elt));
      return this->end() - 1;
    }

    assert(i >= this->begin() && "Insertion iterator is out of bounds.");
    assert(i <= this->end() && "Inserting past the end of the vector.");

    if (this->end_x_ >= this->capacity_x_) {
      size_t elt_no = i - this->begin();
      this->Grow();
      i = this->begin() + elt_no;
    }

    ::new ((void *)this->end()) T(::std::move(this->back()));
    // Push everything else over.
    std::move_backward(i, this->end() - 1, this->end());
    this->SetEnd(this->end() + 1);

    // If we just moved the element we're inserting, be sure to update
    // the reference.
    T *elt_ptr = &elt;
    if (i <= elt_ptr && elt_ptr < this->end_x_) ++elt_ptr;

    *i = ::std::move(*elt_ptr);
    return i;
  }

  iterator insert(iterator i, const T &elt) {
    if (i == this->end()) {  // Important special case for empty vector.
      this->push_back(elt);
      return this->end() - 1;
    }

    assert(i >= this->begin() && "Insertion iterator is out of bounds.");
    assert(i <= this->end() && "Inserting past the end of the vector.");

    if (this->end_x_ >= this->capacity_x_) {
      size_t elt_no = i - this->begin();
      this->Grow();
      i = this->begin() + elt_no;
    }
    ::new ((void *)this->end()) T(std::move(this->back()));
    // Push everything else over.
    std::move_backward(i, this->end() - 1, this->end());
    this->SetEnd(this->end() + 1);

    // If we just moved the element we're inserting, be sure to update
    // the reference.
    const T *elt_ptr = &elt;
    if (i <= elt_ptr && elt_ptr < this->end_x_) ++elt_ptr;

    *i = *elt_ptr;
    return i;
  }

  iterator insert(iterator i, size_type num_to_insert, const T &elt) {
    // Convert iterator to elt# to avoid invalidating iterator when we reserve()
    size_t insert_elt = i - this->begin();

    if (i == this->end()) {  // Important special case for empty vector.
      append(num_to_insert, elt);
      return this->begin() + insert_elt;
    }

    assert(i >= this->begin() && "Insertion iterator is out of bounds.");
    assert(i <= this->end() && "Inserting past the end of the vector.");

    // Ensure there is enough space.
    reserve(this->size() + num_to_insert);

    // Uninvalidate the iterator.
    i = this->begin() + insert_elt;

    // If there are more elements between the insertion point and the end of the
    // range than there are being inserted, we can use a simple approach to
    // insertion.  Since we already reserved space, we know that this won't
    // reallocate the vector.
    if (size_t(this->end() - i) >= num_to_insert) {
      T *old_end = this->end();
      append(std::move_iterator<iterator>(this->end() - num_to_insert), std::move_iterator<iterator>(this->end()));

      // Copy the existing elements that get replaced.
      std::move_backward(i, old_end - num_to_insert, old_end);

      std::fill_n(i, num_to_insert, elt);
      return i;
    }

    // Otherwise, we're inserting more elements than exist already, and we're
    // not inserting at the end.

    // Move over the elements that we're about to overwrite.
    T *old_end = this->end();
    this->SetEnd(this->end() + num_to_insert);
    size_t num_overwritten = old_end - i;
    this->UninitializedMove(i, old_end, this->end() - num_overwritten);

    // Replace the overwritten part.
    std::fill_n(i, num_overwritten, elt);

    // Insert the non-overwritten middle part.
    std::uninitialized_fill_n(old_end, num_to_insert - num_overwritten, elt);
    return i;
  }

  template <typename TIt>
  iterator insert(iterator i, TIt from, TIt to) {
    // Convert iterator to elt# to avoid invalidating iterator when we reserve()
    size_t insert_elt = i - this->begin();

    if (i == this->end()) {  // Important special case for empty vector.
      append(from, to);
      return this->begin() + insert_elt;
    }

    assert(i >= this->begin() && "Insertion iterator is out of bounds.");
    assert(i <= this->end() && "Inserting past the end of the vector.");

    size_t num_to_insert = std::distance(from, to);

    // Ensure there is enough space.
    reserve(this->size() + num_to_insert);

    // Uninvalidate the iterator.
    i = this->begin() + insert_elt;

    // If there are more elements between the insertion point and the end of the
    // range than there are being inserted, we can use a simple approach to
    // insertion.  Since we already reserved space, we know that this won't
    // reallocate the vector.
    if (size_t(this->end() - i) >= num_to_insert) {
      T *old_end = this->end();
      append(std::move_iterator<iterator>(this->end() - num_to_insert), std::move_iterator<iterator>(this->end()));

      // Copy the existing elements that get replaced.
      std::move_backward(i, old_end - num_to_insert, old_end);

      std::copy(from, to, i);
      return i;
    }

    // Otherwise, we're inserting more elements than exist already, and we're
    // not inserting at the end.

    // Move over the elements that we're about to overwrite.
    T *old_end = this->end();
    this->SetEnd(this->end() + num_to_insert);
    size_t num_overwritten = old_end - i;
    this->UninitializedMove(i, old_end, this->end() - num_overwritten);

    // Replace the overwritten part.
    for (T *j = i; num_overwritten > 0; --num_overwritten) {
      *j = *from;
      ++j;
      ++from;
    }

    // Insert the non-overwritten middle part.
    this->UninitializedCopy(from, to, old_end);
    return i;
  }

  void insert(iterator i, std::initializer_list<T> il) { insert(i, il.begin(), il.end()); }

  template <typename... TArgTypes>
  void emplace_back(TArgTypes &&...args) {
    if (UNLIKELY(this->end_x_ >= this->capacity_x_)) this->Grow();
    ::new ((void *)this->end()) T(std::forward<TArgTypes>(args)...);
    this->SetEnd(this->end() + 1);
  }

  SmallVectorImpl &operator=(const SmallVectorImpl &rhs);

  SmallVectorImpl &operator=(SmallVectorImpl &&rhs);

  bool operator==(const SmallVectorImpl &rhs) const {
    if (this->size() != rhs.size()) return false;
    return std::equal(this->begin(), this->end(), rhs.begin());
  }
  bool operator!=(const SmallVectorImpl &rhs) const { return !(*this == rhs); }

  bool operator<(const SmallVectorImpl &rhs) const {
    return std::lexicographical_compare(this->begin(), this->end(), rhs.begin(), rhs.end());
  }

  /// Set the array size to \p n, which the current array must have enough
  /// capacity for.
  ///
  /// This does not construct or destroy any elements in the vector.
  ///
  /// Clients can use this in conjunction with capacity() to write past the end
  /// of the buffer when they know that more elements are available, and only
  /// update the size later. This avoids the cost of value initializing elements
  /// which will only be overwritten.
  void set_size(size_type n) {
    assert(n <= this->capacity());
    this->SetEnd(this->begin() + n);
  }
};

template <typename T>
void SmallVectorImpl<T>::swap(SmallVectorImpl<T> &rhs) {
  if (this == &rhs) return;

  // We can only avoid copying elements if neither vector is small.
  if (!this->IsSmall() && !rhs.IsSmall()) {
    std::swap(this->begin_x_, rhs.begin_x_);
    std::swap(this->end_x_, rhs.end_x_);
    std::swap(this->capacity_x_, rhs.capacity_x_);
    return;
  }
  if (rhs.size() > this->capacity()) this->Grow(rhs.size());
  if (this->size() > rhs.capacity()) rhs.Grow(this->size());

  // Swap the shared elements.
  size_t num_shared = this->size();
  if (num_shared > rhs.size()) num_shared = rhs.size();
  for (size_type i = 0; i != num_shared; ++i) std::swap((*this)[i], rhs[i]);

  // Copy over the extra elts.
  if (this->size() > rhs.size()) {
    size_t elt_diff = this->size() - rhs.size();
    this->UninitializedCopy(this->begin() + num_shared, this->end(), rhs.end());
    rhs.SetEnd(rhs.end() + elt_diff);
    this->DestroyRange(this->begin() + num_shared, this->end());
    this->SetEnd(this->begin() + num_shared);
  } else if (rhs.size() > this->size()) {
    size_t elt_diff = rhs.size() - this->size();
    this->UninitializedCopy(rhs.begin() + num_shared, rhs.end(), this->end());
    this->SetEnd(this->end() + elt_diff);
    this->DestroyRange(rhs.begin() + num_shared, rhs.end());
    rhs.SetEnd(rhs.begin() + num_shared);
  }
}

template <typename T>
SmallVectorImpl<T> &SmallVectorImpl<T>::operator=(const SmallVectorImpl<T> &rhs) {
  // Avoid self-assignment.
  if (this == &rhs) return *this;

  // If we already have sufficient space, assign the common elements, then
  // destroy any excess.
  size_t rhh_size = rhs.size();
  size_t cur_size = this->size();
  if (cur_size >= rhh_size) {
    // Assign common elements.
    iterator new_end;
    if (rhh_size)
      new_end = std::copy(rhs.begin(), rhs.begin() + rhh_size, this->begin());
    else
      new_end = this->begin();

    // Destroy excess elements.
    this->DestroyRange(new_end, this->end());

    // Trim.
    this->SetEnd(new_end);
    return *this;
  }

  // If we have to Grow to have enough elements, destroy the current elements.
  // This allows us to avoid copying them during the Grow.
  // FIXME: don't do this if they're efficiently moveable.
  if (this->capacity() < rhh_size) {
    // Destroy current elements.
    this->DestroyRange(this->begin(), this->end());
    this->SetEnd(this->begin());
    cur_size = 0;
    this->Grow(rhh_size);
  } else if (cur_size) {
    // Otherwise, use assignment for the already-constructed elements.
    std::copy(rhs.begin(), rhs.begin() + cur_size, this->begin());
  }

  // Copy construct the new elements in place.
  this->UninitializedCopy(rhs.begin() + cur_size, rhs.end(), this->begin() + cur_size);

  // Set end.
  this->SetEnd(this->begin() + rhh_size);
  return *this;
}

template <typename T>
SmallVectorImpl<T> &SmallVectorImpl<T>::operator=(SmallVectorImpl<T> &&rhs) {
  // Avoid self-assignment.
  if (this == &rhs) return *this;

  // If the rhs isn't small, clear this vector and then steal its buffer.
  if (!rhs.IsSmall()) {
    this->DestroyRange(this->begin(), this->end());
    if (!this->IsSmall()) free(this->begin());
    this->begin_x_ = rhs.begin_x_;
    this->end_x_ = rhs.end_x_;
    this->capacity_x_ = rhs.capacity_x_;
    rhs.ResetToSmall();
    return *this;
  }

  // If we already have sufficient space, assign the common elements, then
  // destroy any excess.
  size_t rhh_size = rhs.size();
  size_t cur_size = this->size();
  if (cur_size >= rhh_size) {
    // Assign common elements.
    iterator new_end = this->begin();
    if (rhh_size) new_end = std::move(rhs.begin(), rhs.end(), new_end);

    // Destroy excess elements and trim the bounds.
    this->DestroyRange(new_end, this->end());
    this->SetEnd(new_end);

    // Clear the rhs.
    rhs.clear();

    return *this;
  }

  // If we have to Grow to have enough elements, destroy the current elements.
  // This allows us to avoid copying them during the Grow.
  // FIXME: this may not actually make any sense if we can efficiently move
  // elements.
  if (this->capacity() < rhh_size) {
    // Destroy current elements.
    this->DestroyRange(this->begin(), this->end());
    this->SetEnd(this->begin());
    cur_size = 0;
    this->Grow(rhh_size);
  } else if (cur_size) {
    // Otherwise, use assignment for the already-constructed elements.
    std::move(rhs.begin(), rhs.begin() + cur_size, this->begin());
  }

  // Move-construct the new elements in place.
  this->UninitializedMove(rhs.begin() + cur_size, rhs.end(), this->begin() + cur_size);

  // Set end.
  this->SetEnd(this->begin() + rhh_size);

  rhs.clear();
  return *this;
}

/// Storage for the SmallVector elements which aren't contained in
/// SmallVectorTemplateCommon. There are 'n-1' elements here. The remaining '1'
/// element is in the base class. This is specialized for the n=1 and n=0 cases
/// to avoid allocating unnecessary storage.
template <typename T, unsigned N>
struct SmallVectorStorage {
  typename SmallVectorTemplateCommon<T>::U InlineElts[N - 1];
};
template <typename T>
struct SmallVectorStorage<T, 1> {};
template <typename T>
struct SmallVectorStorage<T, 0> {};

/// This is a 'vector' (really, a variable-sized array), optimized
/// for the case when the array is small.  It contains some number of elements
/// in-place, which allows it to avoid heap allocation when the actual number of
/// elements is below that threshold.  This allows normal "small" cases to be
/// fast without losing generality for large inputs.
///
/// Note that this does not attempt to be exception safe.
///
template <typename T, unsigned N>
class SmallVector : public SmallVectorImpl<T> {
  /// Inline space for elements which aren't stored in the base class.
  SmallVectorStorage<T, N> Storage;

 public:
  SmallVector() : SmallVectorImpl<T>(N) {}

  explicit SmallVector(size_t size, const T &value = T()) : SmallVectorImpl<T>(N) { this->assign(size, value); }

  template <typename TIt>
  SmallVector(TIt s, TIt e) : SmallVectorImpl<T>(N) {
    this->append(s, e);
  }

  SmallVector(std::initializer_list<T> il) : SmallVectorImpl<T>(N) { this->assign(il); }

  SmallVector(const SmallVector &rhs) : SmallVectorImpl<T>(N) {
    if (!rhs.empty()) SmallVectorImpl<T>::operator=(rhs);
  }

  const SmallVector &operator=(const SmallVector &rhs) {
    SmallVectorImpl<T>::operator=(rhs);
    return *this;
  }

  SmallVector(SmallVector &&rhs) : SmallVectorImpl<T>(N) {
    if (!rhs.empty()) SmallVectorImpl<T>::operator=(::std::move(rhs));
  }

  const SmallVector &operator=(SmallVector &&rhs) {
    SmallVectorImpl<T>::operator=(::std::move(rhs));
    return *this;
  }

  SmallVector(SmallVectorImpl<T> &&rhs) : SmallVectorImpl<T>(N) {
    if (!rhs.empty()) SmallVectorImpl<T>::operator=(::std::move(rhs));
  }

  const SmallVector &operator=(SmallVectorImpl<T> &&rhs) {
    SmallVectorImpl<T>::operator=(::std::move(rhs));
    return *this;
  }

  const SmallVector &operator=(std::initializer_list<T> il) {
    this->assign(il);
    return *this;
  }
};

template <typename T, unsigned N>
static inline size_t capacity_in_bytes(const SmallVector<T, N> &x) {
  return x.capacity_in_bytes();
}

}  // namespace memgraph::utils

namespace std {
/// Implement std::swap in terms of SmallVector swap.
template <typename T>
inline void swap(memgraph::utils::SmallVectorImpl<T> &lhs, memgraph::utils::SmallVectorImpl<T> &rhs) {
  lhs.swap(rhs);
}

/// Implement std::swap in terms of SmallVector swap.
template <typename T, unsigned N>
inline void swap(memgraph::utils::SmallVector<T, N> &lhs, memgraph::utils::SmallVector<T, N> &rhs) {
  lhs.swap(rhs);
}
}  // namespace std

namespace memgraph::utils {
/// GrowPod - This is an implementation of the Grow() method which only works
/// on POD-like datatypes and is out of line to reduce code duplication.
inline void SmallVectorBase::GrowPod(void *first_el, size_t min_size_in_bytes, size_t t_size) {
  size_t cur_size_bytes = size_in_bytes();
  size_t new_capacity_in_bytes = 2 * capacity_in_bytes() + t_size;  // Always Grow.
  if (new_capacity_in_bytes < min_size_in_bytes) new_capacity_in_bytes = min_size_in_bytes;

  void *new_elts;
  if (begin_x_ == first_el) {
    new_elts = malloc(new_capacity_in_bytes);

    // Copy the elements over.  No need to run dtors on PODs.
    memcpy(new_elts, this->begin_x_, cur_size_bytes);
  } else {
    // If this wasn't grown from the inline copy, Grow the allocated space.
    new_elts = realloc(this->begin_x_, new_capacity_in_bytes);
  }
  assert(new_elts && "Out of memory");

  this->end_x_ = (char *)new_elts + cur_size_bytes;
  this->begin_x_ = new_elts;
  this->capacity_x_ = (char *)this->begin_x_ + new_capacity_in_bytes;
}
}  // namespace memgraph::utils
