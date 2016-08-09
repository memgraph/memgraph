#include "utils/crtp.hpp"
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

    template <class It>
    class IteratorBase : public Crtp<It>
    {
    protected:
        IteratorBase() : map(nullptr) { index = ~((size_t)0); }
        IteratorBase(const RhHashMap *map) : map(map)
        {
            index = 0;
            while (index < map->capacity && !map->array[index].valid()) {
                index++;
            }
            if (index == map->capacity) {
                map = nullptr;
                index = ~((size_t)0);
            }
        }

        const RhHashMap *map;
        size_t index;

    public:
        IteratorBase(const IteratorBase &) = default;
        IteratorBase(IteratorBase &&) = default;

        D *operator*()
        {
            assert(index < map->capacity && map->array[index].valid());
            return map->array[index].ptr();
        }

        D *operator->()
        {
            assert(index < map->capacity && map->array[index].valid());
            return map->array[index].ptr();
        }

        It &operator++()
        {
            assert(index < map->capacity && map->array[index].valid());
            do {
                index++;
                if (index >= map->capacity) {
                    map = nullptr;
                    index = ~((size_t)0);
                    break;
                }
            } while (!map->array[index].valid());

            return this->derived();
        }

        It &operator++(int) { return operator++(); }

        friend bool operator==(const It &a, const It &b)
        {
            return a.index == b.index && a.map == b.map;
        }

        friend bool operator!=(const It &a, const It &b) { return !(a == b); }
    };

public:
    class ConstIterator : public IteratorBase<ConstIterator>
    {
        friend class RhHashMap;
        ConstIterator(const RhHashMap *map) : IteratorBase<ConstIterator>(map)
        {
        }

    public:
        ConstIterator() = default;
        ConstIterator(const ConstIterator &) = default;

        const D *operator->()
        {
            return IteratorBase<ConstIterator>::operator->();
        }

        const D *operator*()
        {
            return IteratorBase<ConstIterator>::operator*();
        }
    };

    class Iterator : public IteratorBase<Iterator>
    {
        friend class RhHashMap;
        Iterator(const RhHashMap *map) : IteratorBase<Iterator>(map) {}

    public:
        Iterator() = default;
        Iterator(const Iterator &) = default;
    };

    RhHashMap() {}

    RhHashMap(const RhHashMap &other)
    {
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

    ~RhHashMap() { this->clear(); }

    Iterator begin() { return Iterator(this); }

    ConstIterator begin() const { return ConstIterator(this); }

    ConstIterator cbegin() const { return ConstIterator(this); }

    Iterator end() { return Iterator(); }

    ConstIterator end() const { return ConstIterator(); }

    ConstIterator cend() const { return ConstIterator(); }

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

        free(a);
    }

    OptionPtr<D> find(const K &key)
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

    void clear()
    {
        free(array);
        array = nullptr;
        capacity = 0;
        count = 0;
    }

    size_t size() { return count; }

private:
    size_t index(const K &key, size_t mask)
    {
        return hash(std::hash<K>()(key)) & mask;
    }
    size_t hash(size_t x) const
    {
        x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
        x = x ^ (x >> 31);
        return x;
    }

    size_t mask() { return capacity - 1; }

    Combined *array = nullptr;
    size_t capacity = 0;
    size_t count = 0;

    friend class IteratorBase<Iterator>;
    friend class IteratorBase<ConstIterator>;
};
