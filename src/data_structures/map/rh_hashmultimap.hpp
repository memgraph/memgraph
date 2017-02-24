#pragma mark

#include <cstring>
#include <functional>
#include "data_structures/map/rh_common.hpp"
#include "utils/assert.hpp"
#include "utils/crtp.hpp"
#include "utils/likely.hpp"
#include "utils/option.hpp"
#include "utils/option_ptr.hpp"

// HashMultiMap with RobinHood collision resolution policy.
// Single threaded.
// Entrys are POINTERS alligned to 8B.
// Entrys must know thers key.
// D must have method K& get_key()
// K must be comparable with ==.
// HashMap behaves as if it isn't owner of entrys.
//
// Main idea of this MultiMap is a tweak of logic in RobinHood.
// RobinHood offset from prefered slot is equal to the number of slots between
// [current slot and prefered slot>.
// While in this flavour of "multi RobinHood" offset from prefered slot is equal
// to the number of different keyed elements between his current slot and
// prefered slot.
// In the following examples slots will have keys as caracters. So something
// like this: |a| will mean that in this slot there is data with key 'a'.
// like this: | | will mean empty slot.
// like this: |...| will mean arbitary number of slots.
// like this: |b:a| will mean that a want's to be in slot but b is in't.
//
// Examples:
// |...|a:a|...| => off(a) = 0
// |...|a:a|a|...|a|...| => off(a) = 0
// |...|b:a|a|...| => off(a) = 1
// |...|b:a|b|...|b|a|...| => off(a) = 1
// |...|c:a|b|a|...| => off(a) = 2
// |...|c:a|c|...|c|b|...|b||a|...|a|...| => off(a) = 2
// ...
template <class K, class D, size_t init_size_pow2 = 2>
class RhHashMultiMap : public RhBase<K, D, init_size_pow2> {
  typedef RhBase<K, D, init_size_pow2> base;
  using base::array;
  using base::index;
  using base::capacity;
  using base::count;
  using typename base::Combined;
  using base::before_index;
  using base::create_it;

  void increase_size() {
    size_t old_size = capacity;
    auto a = array;
    if (base::increase_size()) {
      for (int i = 0; i < old_size; i++) {
        if (a[i].valid()) {
          add(a[i].ptr());
        }
      }
    }

    free(a);
  }

 public:
  using base::RhBase;
  using base::end;
  using typename base::ConstIterator;
  using typename base::Iterator;

  bool contains(const K &key) const { return find_index(key).is_present(); }

  Iterator find(const K &key_in) {
    auto index = find_index(key_in);
    if (index) {
      return create_it(index.get());
    } else {
      return end();
    }
  }

  ConstIterator find(const K &key_in) const {
    auto index = find_index(key_in);
    if (index) {
      return create_it(index.get());
    } else {
      return end();
    }
  }

 private:
  Option<size_t> find_index(const K &key_in) const {
    if (count > 0) {
      auto key = std::ref(key_in);
      size_t mask = this->mask();
      size_t now = index(key, mask);
      size_t off = 0;
      size_t border = 8 <= capacity ? 8 : capacity;
      Combined other = array[now];
      while (other.valid() && off < border) {
        auto other_off = other.off();
        if (other_off == off && key == other.ptr()->get_key()) {
          return Option<size_t>(now);

        } else if (other_off < off) {  // Other is rich
          break;

        } else {  // Else other has equal or greater off, so he is poor.
          if (UNLIKELY(skip(now, other, other_off, mask))) {
            break;
          }
          off++;
        }
      }
    }

    return Option<size_t>();
  }

 public:
  // Inserts element.
  void add(D *data) { add(data->get_key(), data); }

  // Inserts element with the given key.
  void add(const K &key_in, D *data) {
    debug_assert(key_in == data->get_key(), "Key doesn't match data key.");

    if (count < capacity) {
      auto key = std::ref(key_in);
      size_t mask = this->mask();
      size_t now = index(key, mask);
      size_t start = now;
      size_t off = 0;
      size_t border = 8 <= capacity ? 8 : capacity;

      Combined other = array[now];
      while (off < border) {
        if (other.valid()) {
          const size_t other_off = other.off();
          bool multi = false;
          if (other_off == off && other.ptr()->get_key() == key) {
            // Found the same
            // Must skip same keyd values to insert new value at the
            // end.
            do {
              now = (now + 1) & mask;
              other = array[now];
              if (!other.valid()) {
                // Found empty slot in which data ca be added.
                set(now, data, off);
                return;
              }
            } while (other.equal(key, off));
            // There is no empty slot after same keyed values.
            multi = true;
          } else if (other_off > off ||
                     other_poor(other, mask, start,
                                now)) {         // Else other has equal or
                                                // greater off, so he is poor.
            skip(now, other, other_off, mask);  // TRUE IS IMPOSSIBLE
            off++;
            continue;
          }

          // Data will be insrted at current slot and all other data
          // will be displaced for one slot.
          array[now] = Combined(data, off);
          auto start_insert = now;
          while (is_off_adjusted(other, mask, start_insert, now, multi) ||
                 other.increment_off()) {
            now = (now + 1) & mask;
            auto tmp = array[now];
            array[now] = other;
            other = tmp;
            if (!other.valid()) {
              // Found empty slot which means i can finish now.
              count++;
              return;
            }
          }
          data = other.ptr();
          break;  // Cant insert removed element
        } else {
          // Found empty slot for data.
          set(now, data, off);
          return;
        }
      }
    }

    // There is't enough space for data.
    increase_size();
    add(data);
  }

  // Removes element equal by key and value. Returns true if it existed.
  bool remove(D *data) {
    if (count > 0) {
      auto key = std::ref(data->get_key());
      size_t mask = this->mask();
      size_t now = index(key, mask);
      size_t off = 0;
      size_t border = 8 <= capacity ? 8 : capacity;
      Combined other = array[now];

      while (other.valid() && off < border) {
        const size_t other_off = other.off();
        if (other_off == off && key == other.ptr()->get_key()) {
          // Found same key data.
          auto founded = capacity;
          size_t started = now;
          bool multi = false;
          // Must find slot with searched data.
          do {
            if (other.ptr() == data) {
              // founded it.
              founded = now;
            }
            now = (now + 1) & mask;
            other = array[now];
            if (!other.valid() || UNLIKELY(started == now)) {
              // Reason is possibility of map full of same values.
              break;
            }
          } while (other.equal(key, off) && (multi = true));

          if (founded == capacity) {
            // Didn't found the data.
            return false;
          }

          // Data will be removed by moving other data by one slot
          // before.
          auto bef = before_index(now, mask);
          array[founded] = array[bef];

          auto start_rem = bef;
          while (other.valid() && (is_off_adjusted_rem(other, mask, start_rem,
                                                       bef, now, multi) ||
                                   other.decrement_off())) {
            array[bef] = other;
            bef = now;
            now = (now + 1) & mask;
            other = array[now];
          }

          array[bef] = Combined();
          count--;
          return true;

        } else if (other_off < off) {  // Other is rich
          break;

        } else {  // Else other has equal or greater off, so he is poor.
          // Must skip values of same keys but different key than
          // data.
          if (UNLIKELY(skip(now, other, other_off, mask))) {
            break;
          }
          off++;
        }
      }
    }
    return false;
  }

 private:
  // Skips same key valus as other. true if whole map is full of same key
  // values.
  bool skip(size_t &now, Combined &other, size_t other_off, size_t mask) const {
    auto other_key = other.ptr()->get_key();
    size_t start = now;
    do {
      now = (now + 1) & mask;
      other = array[now];
      if (UNLIKELY(start == now)) {  // Reason is possibility of map
                                     // full of same values.
        return true;
      }
    } while (other.valid() && other.equal(other_key, other_off));
    return false;
  }

  void set(size_t now, D *data, size_t off) {
    array[now] = Combined(data, off);
    count++;
  }

  // True if no adjusment is needed, false otherwise.
  bool is_off_adjusted(Combined &com, size_t mask, size_t start, size_t now,
                       bool multi) {
    if (com.off() == 0) {  // Must be adjusted
      return false;
    }
    size_t cin = index(com.ptr()->get_key(), mask);
    if (outside(start, now, cin)) {  // Outside [start,now] interval
      return multi;
    }
    auto a = array[cin];
    auto b = array[(cin + 1) & mask];
    return a == b;
    // Check if different key has eneterd in to
    // range of other.
  }

  bool other_poor(Combined other, size_t mask, size_t start, size_t now) {
    // If other index is smaller then he is poorer.
    return outside_left_weak(start, now, index(other.ptr()->get_key(), mask));
  }

  // True if no adjusment is needed, false otherwise.
  bool is_off_adjusted_rem(Combined &com, size_t mask, size_t start, size_t bef,
                           size_t now, bool multi) {
    if (com.off() == 0) {  // Must be adjusted
      return false;
    }
    size_t cin = index(com.ptr()->get_key(), mask);
    if (cin == bef) {
      return false;
    }
    if (outside(start, now, cin)) {
      return multi;
    }
    auto a = array[cin];
    auto b = array[before_index(cin, mask)];
    return b.valid() && a == b;
    // Check if different key has eneterd in to
    // range of other.
  }

  // True if p is uutside [start,end] interval
  bool outside(size_t start, size_t end, size_t p) {
    return (start <= end && (p < start || p > end)) ||
           (end < start && p < start && p > end);
  }

  // True if p is outside <start,end] interval
  bool outside_left_weak(size_t start, size_t end, size_t p) {
    return (start <= end && (p <= start || p > end)) ||
           (end < start && p <= start && p > end);
  }
};
