#pragma once
#include "utils/crtp.hpp"
#include "utils/option_ptr.hpp"
#include <cstring>
#include <functional>

// RobinHood base.
// Entrys are POINTERS alligned to 8B.
// Entrys must know thers key.
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

        Combined(D *data, size_t off)
        {
            // assert((((size_t)data) & 0x7) == 0 && off < 8);
            this->data = ((size_t)data) | off;
        }

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

    template <class It>
    class IteratorBase : public Crtp<It>
    {
    protected:
        IteratorBase() : map(nullptr) { advanced = index = ~((size_t)0); }
        IteratorBase(const RhBase *map) : map(map)
        {
            index = 0;
            while (index < map->capacity && !map->array[index].valid()) {
                index++;
            }
            if (index == map->capacity) {
                map = nullptr;
                advanced = index = ~((size_t)0);
            } else {
                advanced = index;
            }
        }
        IteratorBase(const RhBase *map, size_t start)
            : map(map), index(start), advanced(0)
        {
        }

        const RhBase *map;
        size_t advanced;
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
                    map = nullptr;
                    advanced = index = ~((size_t)0);
                    break;
                }
                index = (index + 1) & mask;
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

    RhBase(const RhBase &other)
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

    ~RhBase() { this->clear(); }

    Iterator begin() { return Iterator(this); }

    ConstIterator begin() const { return ConstIterator(this); }

    ConstIterator cbegin() const { return ConstIterator(this); }

    Iterator end() { return Iterator(); }

    ConstIterator end() const { return ConstIterator(); }

    ConstIterator cend() const { return ConstIterator(); }

protected:
    void init_array(size_t size)
    {
        size_t bytes = sizeof(Combined) * size;
        array = (Combined *)malloc(bytes);
        std::memset(array, 0, bytes);
        capacity = size;
    }

    // True if before array has some values.
    // Before array has to be released also.
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

public:
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

    // This is rather expensive but offers good distribution.
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
