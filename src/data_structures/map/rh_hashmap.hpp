#include <functional>

#include "data_structures/map/rh_common.hpp"
#include "utils/crtp.hpp"
#include "utils/option_ptr.hpp"

// HashMap with RobinHood collision resolution policy.
// Single threaded.
// Entrys are saved as pointers alligned to 8B.
// Entrys must know thers key.
// D must have method const K & get_key()
// K must be comparable with ==.
// HashMap behaves as if it isn't owner of entrys.
template <class K, class D, size_t init_size_pow2 = 2>
class RhHashMap : public RhBase<K, D, init_size_pow2> {
  typedef RhBase<K, D, init_size_pow2> base;
  using base::array;
  using base::index;
  using base::capacity;
  using base::count;
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
