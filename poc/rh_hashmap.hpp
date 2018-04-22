#pragma once

#include <cstring>
#include <functional>

#include "glog/logging.h"

#include "option_ptr.hpp"
#include "utils/crtp.hpp"

// RobinHood base.
// Entries are POINTERS alligned to 8B.
// Entries must know thers key.
// D must have method K& get_key()
// K must be comparable with ==.
template <class K, class D, size_t init_size_pow2 = 2>
class RhBase {
 protected:
  class Combined {
   public:
    Combined() : data(0) {}

    Combined(D *data, size_t off) { this->data = ((size_t)data) | off; }

    bool valid() const { return data != 0; }

    size_t off() const { return data & 0x7; }

    void decrement_off_unsafe() { data--; }

    bool decrement_off() {
      if (off() > 0) {
        data--;
        return true;
      }
      return false;
    }

    bool increment_off() {
      if (off() < 7) {
        data++;
        return true;
      }
      return false;
    }

    D *ptr() const { return (D *)(data & (~(0x7))); }

    bool equal(const K &key, size_t off) {
      return this->off() == off && key == ptr()->get_key();
    }

    friend bool operator==(const Combined &a, const Combined &b) {
      return a.off() == b.off() && a.ptr()->get_key() == b.ptr()->get_key();
    }

    friend bool operator!=(const Combined &a, const Combined &b) {
      return !(a == b);
    }

   private:
    size_t data;
  };

  // Base for all iterators. It can start from any point in map.
  template <class It>
  class IteratorBase : public utils::Crtp<It> {
   protected:
    IteratorBase() : map(nullptr) { advanced = index = ~((size_t)0); }
    IteratorBase(const RhBase *map) {
      index = 0;
      while (index < map->capacity && !map->array[index].valid()) {
        index++;
      }
      if (index >= map->capacity) {
        this->map = nullptr;
        advanced = index = ~((size_t)0);
      } else {
        this->map = map;
        advanced = index;
      }
    }
    IteratorBase(const RhBase *map, size_t start)
        : map(map), advanced(0), index(start) {}

    const RhBase *map;

    // How many times did whe advance.
    size_t advanced;

    // Current position in array
    size_t index;

   public:
    IteratorBase(const IteratorBase &) = default;
    IteratorBase(IteratorBase &&) = default;

    D *operator*() {
      DCHECK(index < map->capacity && map->array[index].valid())
          << "Either index is invalid or data is not valid.";
      return map->array[index].ptr();
    }

    D *operator->() {
      DCHECK(index < map->capacity && map->array[index].valid())
          << "Either index is invalid or data is not valid.";
      return map->array[index].ptr();
    }

    It &operator++() {
      DCHECK(index < map->capacity && map->array[index].valid())
          << "Either index is invalid or data is not valid.";
      auto mask = map->mask();
      do {
        advanced++;
        if (advanced >= map->capacity) {
          // Whe have advanced more than the capacity of map is so whe
          // are done.
          map = nullptr;
          advanced = index = ~((size_t)0);
          break;
        }
        index = (index + 1) & mask;
      } while (!map->array[index].valid());  // Check if there is element
                                             // at current position.

      return this->derived();
    }

    It &operator++(int) { return operator++(); }

    friend bool operator==(const It &a, const It &b) {
      return a.index == b.index && a.map == b.map;
    }

    friend bool operator!=(const It &a, const It &b) { return !(a == b); }
  };

 public:
  class ConstIterator : public IteratorBase<ConstIterator> {
    friend class RhBase;

   protected:
    ConstIterator(const RhBase *map) : IteratorBase<ConstIterator>(map) {}
    ConstIterator(const RhBase *map, size_t index)
        : IteratorBase<ConstIterator>(map, index) {}

   public:
    ConstIterator() = default;
    ConstIterator(const ConstIterator &) = default;

    const D *operator->() { return IteratorBase<ConstIterator>::operator->(); }

    const D *operator*() { return IteratorBase<ConstIterator>::operator*(); }
  };

  class Iterator : public IteratorBase<Iterator> {
    friend class RhBase;

   protected:
    Iterator(const RhBase *map) : IteratorBase<Iterator>(map) {}
    Iterator(const RhBase *map, size_t index)
        : IteratorBase<Iterator>(map, index) {}

   public:
    Iterator() = default;
    Iterator(const Iterator &) = default;
  };

  RhBase() {}

  RhBase(const RhBase &other) { copy_from(other); }

  RhBase(RhBase &&other) { take_from(std::move(other)); }

  ~RhBase() { this->clear(); }

  RhBase &operator=(const RhBase &other) {
    clear();
    copy_from(other);
    return *this;
  }

  RhBase &operator=(RhBase &&other) {
    clear();
    take_from(std::move(other));
    return *this;
  }

  Iterator begin() { return Iterator(this); }

  ConstIterator begin() const { return ConstIterator(this); }

  ConstIterator cbegin() const { return ConstIterator(this); }

  Iterator end() { return Iterator(); }

  ConstIterator end() const { return ConstIterator(); }

  ConstIterator cend() const { return ConstIterator(); }

 protected:
  // Copys RAW BYTE data from other RhBase.
  void copy_from(const RhBase &other) {
    capacity = other.capacity;
    count = other.count;
    if (capacity > 0) {
      size_t bytes = sizeof(Combined) * capacity;
      array = (Combined *)malloc(bytes);
      memcpy(array, other.array, bytes);

    } else {
      array = nullptr;
    }
  }

  // Takes data from other RhBase.
  void take_from(RhBase &&other) {
    capacity = other.capacity;
    count = other.count;
    array = other.array;
    other.array = nullptr;
    other.count = 0;
    other.capacity = 0;
  }

  // Initiazes array with given capacity.
  void init_array(size_t capacity) {
    size_t bytes = sizeof(Combined) * capacity;
    array = (Combined *)malloc(bytes);
    std::memset(array, 0, bytes);
    this->capacity = capacity;
  }

  // True if before array has some values.
  // Before array must be released in the caller.
  bool increase_size() {
    if (capacity == 0) {
      // assert(array == nullptr && count == 0);
      size_t new_size = 1 << init_size_pow2;
      init_array(new_size);
      return false;
    }
    size_t new_size = capacity * 2;
    init_array(new_size);
    count = 0;
    return true;
  }

  Iterator create_it(size_t index) { return Iterator(this, index); }
  ConstIterator create_it(size_t index) const {
    return ConstIterator(this, index);
  }

 public:
  // Cleares all data.
  void clear() {
    free(array);
    array = nullptr;
    capacity = 0;
    count = 0;
  }

  size_t size() const { return count; }

 protected:
  size_t before_index(size_t now, size_t mask) {
    return (now - 1) & mask;  // THIS IS VALID
  }

  size_t index(const K &key, size_t mask) const {
    return hash(std::hash<K>()(key)) & mask;
  }

  // NOTE: This is rather expensive but offers good distribution.
  size_t hash(size_t x) const {
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
  }

  size_t mask() const { return capacity - 1; }

  Combined *array = nullptr;
  size_t capacity = 0;
  size_t count = 0;

  friend class IteratorBase<Iterator>;
  friend class IteratorBase<ConstIterator>;
};

/**
 * HashMap with RobinHood collision resolution policy.
 * Single threaded.
 * Entries are saved as pointers alligned to 8B.
 * Entries must know thers key.
 * D must have method const K & get_key()
 * K must be comparable with ==.
 * HashMap behaves as if it isn't owner of entries.
 * BE CAREFUL - this structure assumes that the pointer to Data is 8-alligned!
 */
template <class K, class D, size_t init_size_pow2 = 2>
class RhHashMap : public RhBase<K, D, init_size_pow2> {
  typedef RhBase<K, D, init_size_pow2> base;
  using base::array;
  using base::capacity;
  using base::count;
  using base::index;
  using typename base::Combined;

  void increase_size() {
    size_t old_size = capacity;
    auto a = array;
    if (base::increase_size()) {
      for (int i = 0; i < old_size; i++) {
        if (a[i].valid()) {
          insert(a[i].ptr());
        }
      }
    }

    free(a);
  }

 public:
  using base::RhBase;

  bool contains(const K &key) { return find(key).is_present(); }

  OptionPtr<D> find(const K key) {
    size_t mask = this->mask();
    size_t now = index(key, mask);
    size_t off = 0;
    size_t border = 8 <= capacity ? 8 : capacity;

    while (off < border) {
      Combined other = array[now];
      if (other.valid()) {
        auto other_off = other.off();
        if (other_off == off && key == other.ptr()->get_key()) {
          // Found data.
          return OptionPtr<D>(other.ptr());

        } else if (other_off < off) {  // Other is rich
          break;
        }  // Else other has equal or greater offset, so he is poor.
      } else {
        // Empty slot means that there is no searched data.
        break;
      }

      off++;
      now = (now + 1) & mask;
    }
    return OptionPtr<D>();
  }

  // Inserts element. Returns true if element wasn't in the map.
  bool insert(D *data) {
    CHECK(!(((uint64_t) static_cast<void *>(data) & 7)))
        << "Data is not 8-alligned.";
    if (count < capacity) {
      size_t mask = this->mask();
      auto key = std::ref(data->get_key());
      size_t now = index(key, mask);
      size_t off = 0;
      size_t border = 8 <= capacity ? 8 : capacity;

      while (off < border) {
        Combined other = array[now];
        if (other.valid()) {
          auto other_off = other.off();
          if (other_off == off && key == other.ptr()->get_key()) {
            // Element already exists.
            return false;

          } else if (other_off < off) {  // Other is rich
            // Set data.
            array[now] = Combined(data, off);

            // Move other data to the higher indexes,
            while (other.increment_off()) {
              now = (now + 1) & mask;
              auto tmp = array[now];
              array[now] = other;
              other = tmp;
              if (!other.valid()) {
                count++;
                return true;
              }
            }
            data = other.ptr();
            break;  // Cant insert removed element because it would
                    // be to far from his real place.
          }         // Else other has equal or greater offset, so he is poor.
        } else {
          // Data can be placed in this empty slot.
          array[now] = Combined(data, off);
          count++;
          return true;
        }

        off++;
        now = (now + 1) & mask;
      }
    }

    // There isn't enough space for element pointed by data so whe must
    // increase array.
    increase_size();
    return insert(data);
  }

  // Removes element. Returns removed element if it existed.
  OptionPtr<D> remove(const K &key) {
    size_t mask = this->mask();
    size_t now = index(key, mask);
    size_t off = 0;
    size_t border = 8 <= capacity ? 8 : capacity;

    while (off < border) {
      Combined other = array[now];
      if (other.valid()) {
        auto other_off = other.off();
        auto other_ptr = other.ptr();
        if (other_off == off && key == other_ptr->get_key()) {  // Found it

          auto before = now;
          // Whe must move other elements one slot lower.
          do {
            // This is alright even for off=0 on found element
            // because it wont be seen.
            other.decrement_off_unsafe();

            array[before] = other;
            before = now;
            now = (now + 1) & mask;
            other = array[now];
          } while (other.valid() &&
                   other.off() > 0);  // Exit if whe encounter empty
                                      // slot or data which is exactly
                                      // in slot which it want's to be.

          array[before] = Combined();
          count--;
          return OptionPtr<D>(other_ptr);

        } else if (other_off < off) {  // Other is rich
          break;
        }  // Else other has equal or greater offset, so he is poor.
      } else {
        // If the element to be removed existed in map it would be here.
        break;
      }

      off++;
      now = (now + 1) & mask;
    }
    return OptionPtr<D>();
  }
};
