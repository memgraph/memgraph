#pragma once

#include <memory>

#include "utils/assert.hpp"

namespace utils {

/**
 * Helper class for implementing copy-on-write member variables. Memory
 * management is automatic via shared-ptr: it is allowed for the original
 * owner to expire, the content will remain valid in copies.
 *
 * This class is generally not thread-safe. However, it is intended for use in
 * Memgraph's MVCC which is concurrent, but with specific guarantees that also
 * make it possible to use this. These guarantees are:
 *   1. Writing can only happen when there are no copies (no parallel reads). An
 * implication of this is that an obtained copy is immutable, even though in
 * general CopyOnWrite does not guarantee this (generally the owner's
 * modificatins will be visible to the copies).
 *   2. Copies are not created in parallel. MVCC guarantees this with
 * record-locking, but should be in fact legal in CopyOnWrite.
 *
 * @tparam TElement - type of content. Must be copy-constructable.
 */
template <typename TElement>
class CopyOnWrite {
 public:
  /**
   * Creates a new CopyOnWrite that owns it's element.
   *
   * @param args - Arguments forwarded to the TElement constructor.
   * @tparam TArgs - Argument types.
   */
  template <typename... TArgs>
  CopyOnWrite(TArgs &&... args) {
    TElement *new_element = new TElement(std::forward<TArgs>(args)...);
    element_ptr_.reset(new_element);
    is_owner_ = true;
  }

  /** Creates a copy of the given CopyOnWrite object that does not copy the
   * element and does not assume ownership over it. */
  CopyOnWrite(const CopyOnWrite &other)
      : element_ptr_{other.element_ptr_}, is_owner_{false} {}

  /** Creates a copy of the given CopyOnWrite object that does not copy the
   * element and does not assume ownership over it. This is a non-const
   * reference accepting copy constructor. The hack is necessary to prevent the
   * variadic constructor from being a better match for non-const CopyOnWrite
   * argument. */
  CopyOnWrite(CopyOnWrite &other)
      : CopyOnWrite(const_cast<const CopyOnWrite &>(other)) {}

  /** Creates a copy of the given temporary CyopOnWrite object. If the temporary
   * is owner then ownership is transferred to this CopyOnWrite. Otherwise this
   * CopyOnWrite does not become the owner. */
  CopyOnWrite(CopyOnWrite &&other) = default;

  /** Copy assignment of another CopyOnWrite. Does not transfer ownership (this
   * CopyOnWrite is not the owner). */
  CopyOnWrite &operator=(const CopyOnWrite &other) {
    element_ptr_ = other.element_ptr_;
    is_owner_ = false;
    return *this;
  }

  /** Copy assignment of a temporary CopyOnWrite. If the temporary is owner then
   * ownerships is transferred to this CopyOnWrite. Otherwise this CopyOnWrite
   * does not become the owner. */
  CopyOnWrite &operator=(CopyOnWrite &&other) = default;

  // All the dereferencing operators are overloaded to return a const element
  // reference. There is no differentiation between const and non-const member
  // function behavior because an implicit copy creation on non-const
  // dereferencing would most likely result in excessive copying. For that
  // reason
  // an explicit call to the `Write` function is required to obtain a non-const
  // reference to element.
  const TElement &operator*() { return *element_ptr_; }
  const TElement &operator*() const { return *element_ptr_; }
  const TElement &get() { return *element_ptr_; }
  const TElement &get() const { return *element_ptr_; }
  const TElement *operator->() { return element_ptr_.get(); }
  const TElement *operator->() const { return element_ptr_.get(); }

  /** Indicates if this CopyOnWrite object is the owner of it's element. */
  bool is_owner() const { return is_owner_; };

  /**
   * If this CopyOnWrite is the owner of it's element, a non-const reference to
   * is returned. If this CopyOnWrite is not the owner, then the element is
   * copied and this CopyOnWrite becomes the owner.
   */
  TElement &Write() {
    if (is_owner_) return *element_ptr_;
    element_ptr_ = std::shared_ptr<TElement>(new TElement(*element_ptr_));
    is_owner_ = true;
    return *element_ptr_;
  };

 private:
  std::shared_ptr<TElement> element_ptr_;
  bool is_owner_{false};
};
}  // namespace utils
