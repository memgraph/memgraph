#pragma once
#include <cassert>
#include <cstring>
#include <functional>
#include "utils/crtp.hpp"
#include "utils/option_ptr.hpp"

// RobinHood base.
// Entries are POINTERS alligned to 8B.
// Entries must know thers key.
// D must have method K& get_key()
// K must be comparable with ==.
template <class K, class D, size_t init_size_pow2 = 2>
class RhBase
{
protected:
    class Combined
    {

    public:
        Combined() : data(0) {}

        Combined(D *data, size_t off) { this->data = ((size_t)data) | off; }

        bool valid() const { return data != 0; }

        size_t off() const { return data & 0x7; }

        void decrement_off_unsafe() { data--; }

        bool decrement_off()
        {
            if (off() > 0) {
                data--;
                return true;
            }
            return false;
        }

        bool increment_off()
        {
            if (off() < 7) {
                data++;
                return true;
            }
            return false;
        }

        D *ptr() const { return (D *)(data & (~(0x7))); }

        bool equal(const K &key, size_t off)
        {
            return this->off() == off && key == ptr()->get_key();
        }

        friend bool operator==(const Combined &a, const Combined &b)
        {
            return a.off() == b.off() &&
                   a.ptr()->get_key() == b.ptr()->get_key();
        }

        friend bool operator!=(const Combined &a, const Combined &b)
        {
            return !(a == b);
        }

    private:
        size_t data;
    };

    // Base for all iterators. It can start from any point in map.
    template <class It>
    class IteratorBase : public Crtp<It>
    {
    protected:
        IteratorBase() : map(nullptr) { advanced = index = ~((size_t)0); }
        IteratorBase(const RhBase *map)
        {
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
            : map(map), index(start), advanced(0)
        {
        }

        const RhBase *map;

        // How many times did whe advance.
        size_t advanced;

        // Current position in array
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
            } while (!map->array[index].valid()); // Check if there is element
                                                  // at current position.

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
        friend class RhBase;

    protected:
        ConstIterator(const RhBase *map) : IteratorBase<ConstIterator>(map) {}
        ConstIterator(const RhBase *map, size_t index)
            : IteratorBase<ConstIterator>(map, index)
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
        friend class RhBase;

    protected:
        Iterator(const RhBase *map) : IteratorBase<Iterator>(map) {}
        Iterator(const RhBase *map, size_t index)
            : IteratorBase<Iterator>(map, index)
        {
        }

    public:
        Iterator() = default;
        Iterator(const Iterator &) = default;
    };

    RhBase() {}

    RhBase(const RhBase &other) { copy_from(other); }

    RhBase(RhBase &&other) { take_from(std::move(other)); }

    ~RhBase() { this->clear(); }

    RhBase &operator=(const RhBase &other)
    {
        clear();
        copy_from(other);
        return *this;
    }

    RhBase &operator=(RhBase &&other)
    {
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
    void copy_from(const RhBase &other)
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

    // Takes data from other RhBase.
    void take_from(RhBase &&other)
    {
        capacity = other.capacity;
        count = other.count;
        array = other.array;
        other.array = nullptr;
        other.count = 0;
        other.capacity = 0;
    }

    // Initiazes array with given capacity.
    void init_array(size_t capacity)
    {
        size_t bytes = sizeof(Combined) * capacity;
        array = (Combined *)malloc(bytes);
        std::memset(array, 0, bytes);
        this->capacity = capacity;
    }

    // True if before array has some values.
    // Before array must be released in the caller.
    bool increase_size()
    {
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
    ConstIterator create_it(size_t index) const
    {
        return ConstIterator(this, index);
    }

public:
    // Cleares all data.
    void clear()
    {
        free(array);
        array = nullptr;
        capacity = 0;
        count = 0;
    }

    size_t size() const { return count; }

protected:
    size_t before_index(size_t now, size_t mask)
    {
        return (now - 1) & mask; // THIS IS VALID
    }

    size_t index(const K &key, size_t mask) const
    {
        return hash(std::hash<K>()(key)) & mask;
    }

    // NOTE: This is rather expensive but offers good distribution.
    size_t hash(size_t x) const
    {
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
