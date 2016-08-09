#include "utils/option_ptr.hpp"

// HashMap with RobinHood collision resolution policy.
// Single threaded.
// Entrys are saved as pointers alligned to 8B.
// Entrys must know thers key.
// D must have method K& get_key()
// K must be comparable with ==.
// HashMap behaves as if it isn't owner of entrys.
template <class K, class D, size_t init_size_pow2 = 2>
class RhHashMap
{
private:
    class Combined
    {

    public:
        Combined() : data(0) {}

        Combined(D *data, size_t off)
        {
            assert((data & 0x7) == 0 && off < 8);
            this->data = ((size_t)data) | off;
        }

        bool valid() { return data != 0; }

        size_t off() { return data & 0x7; }

        D *ptr() { return (D *)(data & (~(0x7))); }

    private:
        size_t data;
    };

    void init_array(size_t size)
    {
        size_t bytes = sizeof(Combined) * size;
        array = (Combined *)malloc(bytes);
        memset(array, 0, bytes);
        capacity = size;
    }

    void increase_size()
    {
        if (capacity == 0) {
            assert(array == nullptr && count == 0);
            size_t new_size = 1 << init_size_pow2;
            init_array(new_size);
            return;
        }
        size_t new_size = capacity * 2;
        size_t old_size = capacity;
        auto a = array;
        init_array(new_size);
        count = 0;

        for (int i = 0; i < old_size; i++) {
            if (a[i].valid()) {
                insert(a[i].ptr());
            }
        }

        delete[] a;
    }

public:
    RhHashMap() {}

    OptionPtr<D> get(const K &key)
    {
        size_t mask = this->mask();
        size_t now = index(key, mask);
        size_t off = 0;
        size_t border = 8 <= capacity ? 8 : capacity;
        while (off < border) {
            Combined other = array[now];
            if (other.valid()) {
                auto other_off = other.off();
                if (other_off == off && key == other.ptr()->get_key()) {
                    return OptionPtr<D>(other.ptr());

                } else if (other_off < off) { // Other is rich
                    break;
                } // Else other has equal or greater offset, so he is poor.
            } else {
                break;
            }

            off++;
            now = (now + 1) & mask;
        }
        return OptionPtr<D>();
    }

    // Inserts element. Returns true if element wasn't in the map.
    bool insert(D *data)
    {
        size_t mask = this->mask();
        auto key = data->get_key();
        size_t now = index(key, mask);
        size_t off = 0;
        size_t border = 8 <= capacity ? 8 : capacity;
        while (off < border) {
            Combined other = array[now];
            if (other.valid()) {
                auto other_off = other.off();
                if (other_off == off && key == other.ptr()->get_key()) {
                    return false;

                } else if (other_off < off) { // Other is rich
                    array[now] = Combined(data, off);

                    // Hacked reusing of function
                    data = other.ptr();
                    key = data->get_key();
                    off = other_off;
                } // Else other has equal or greater offset, so he is poor.
            } else {
                array[now] = Combined(data, off);
                count++;
                return true;
            }

            off++;
            now = (now + 1) & mask;
        }

        increase_size();
        return insert(data);
    }

    size_t size() { return count; }

private:
    size_t index(const K &key, size_t mask)
    {
        return std::hash<K>()(key) & mask;
    }
    size_t mask() { return capacity - 1; }

    Combined *array = nullptr;
    size_t capacity = 0;
    size_t count = 0;
};
