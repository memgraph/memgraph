#include "utils/crtp.hpp"
#include "utils/option_ptr.hpp"
#include <cstring>
#include <functional>

// HashMultiMap with RobinHood collision resolution policy.
// Single threaded.
// Entrys are POINTERS alligned to 8B.
// Entrys must know thers key.
// D must have method K& get_key()
// K must be comparable with ==.
// HashMap behaves as if it isn't owner of entrys.
template <class K, class D, size_t init_size_pow2 = 2>
class RhHashMultiMap
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

        void decrement_off() { data--; }

        bool increment_off()
        {
            if (off() < 7) {
                data++;
                return true;
            }
            return false;
        }

        D *ptr() { return (D *)(data & (~(0x7))); }

    private:
        size_t data;
    };

    template <class It>
    class IteratorBase : public Crtp<It>
    {
    protected:
        IteratorBase() : map(nullptr) { advanced = index = ~((size_t)0); }
        IteratorBase(const RhHashMultiMap *map) : map(map)
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
        IteratorBase(const RhHashMultiMap *map, size_t start)
            : map(map), index(start), advanced(0)
        {
        }

        const RhHashMultiMap *map;
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
        //
        // // True if value is present
        // bool is_present() { return map != nullptr; }

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
        friend class RhHashMultiMap;
        ConstIterator(const RhHashMultiMap *map)
            : IteratorBase<ConstIterator>(map)
        {
        }
        ConstIterator(const RhHashMultiMap *map, size_t index)
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
        friend class RhHashMultiMap;
        Iterator(const RhHashMultiMap *map) : IteratorBase<Iterator>(map) {}
        Iterator(const RhHashMultiMap *map, size_t index)
            : IteratorBase<Iterator>(map, index)
        {
        }

    public:
        Iterator() = default;
        Iterator(const Iterator &) = default;
    };

    RhHashMultiMap() {}

    RhHashMultiMap(const RhHashMultiMap &other)
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

    // RhHashMultiMap(RhHashMultiMap &&other)
    // {
    //     capacity = other.capacity;
    //     count = other.count;
    //     array = other.array;
    //
    //     other.array = nullptr;
    //     other.capacity = 0;
    //     other.count = 0;
    // }

    ~RhHashMultiMap() { this->clear(); }

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
        std::memset(array, 0, bytes);
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
                add(a[i].ptr());
            }
        }

        free(a);
    }

    bool contains(const K &key) { return find(key) != end(); }

    Iterator find(const K &key_in)
    {
        if (count > 0) {
            auto key = std::ref(key_in);
            size_t mask = this->mask();
            size_t now = index(key, mask);
            size_t off = 0;
            size_t checked = 0;
            size_t border = 8 <= capacity ? 8 : capacity;
            Combined other = array[now];
            while (other.valid() && off < border) {
                auto other_off = other.off();
                if (other_off == off && key == other.ptr()->get_key()) {
                    return Iterator(this, now);

                } else if (other_off < off) { // Other is rich
                    break;

                } else { // Else other has equal or greater
                         // offset, so he is poor.
                    auto other_key = other.ptr()->get_key();
                    do {
                        now = (now + 1) & mask;
                        other = array[now];
                        checked++;
                        if (checked >= count) { // Reason is possibility of map
                                                // full of same values.
                            break;
                        }
                    } while (other.valid() && other.off() == other_off &&
                             other.ptr()->get_key() == other_key);
                    off++;
                }
            }
        }

        return end();
    }

    // Inserts element with the given key.
    void add(const K &key_in, D *data)
    {
        assert(key_in == data->get_key());

        if (count < capacity) {
            auto key = std::ref(key_in);
            size_t mask = this->mask();
            size_t now = index(key, mask);
            size_t start = now;
            size_t off = 0;
            size_t border = 8 <= capacity ? 8 : capacity;
            bool multi = false;

            Combined other = array[now];
            while (off < border) {
                if (other.valid()) {
                    auto other_off = other.off();
                    if (other_off == off &&
                        other.ptr()->get_key() == key) { // Found the
                        do {                             // same
                            now = (now + 1) & mask;
                            other = array[now];
                            if (!other.valid()) {
                                set(now, data, off);
                                return;
                            }
                            other_off = other.off();
                        } while (other_off == off &&
                                 other.ptr()->get_key() == key);
                        multi = true;
                    } else if (other_off > off ||
                               other_poor(other, mask, start,
                                          now)) { // Other is poor or the same
                        auto other_key = other.ptr()->get_key();

                        do {
                            now = (now + 1) & mask;
                            other = array[now];
                        } while (other.valid() && other.off() == other_off &&
                                 other.ptr()->get_key() == other_key);
                        off++;
                        continue;
                    }

                    array[now] = Combined(data, off);
                    auto start = now;
                    while (adjust_off(other, mask, start, now, multi)) {
                        now = (now + 1) & mask;
                        auto tmp = array[now];
                        array[now] = other;
                        other = tmp;
                        if (!other.valid()) {
                            count++;
                            return;
                        }
                    }
                    data = other.ptr();
                    break; // Cant insert removed element
                } else {
                    set(now, data, off);
                    return;
                }
            }
        }

        increase_size();
        add(data);
    }

private:
    void set(size_t now, D *data, size_t off)
    {
        array[now] = Combined(data, off);
        count++;
    }

    bool adjust_off(Combined &com, size_t mask, size_t start, size_t now,
                    bool multi)
    {
        if (com.off() == 0) {
            com.increment_off();
            return true;
        }
        size_t cin = index(com.ptr()->get_key(), mask);
        if ((start <= now && (cin < start || cin > now)) ||
            (now < start && cin < start && cin > now)) {
            return multi || com.increment_off();
        }
        auto a = array[cin];
        auto b = array[(cin + 1) & mask];
        return (a.off() == b.off() &&
                a.ptr()->get_key() == b.ptr()->get_key()) ||
               com.increment_off();
    }

    bool other_poor(Combined other, size_t mask, size_t start, size_t now)
    {
        auto cin = index(other.ptr()->get_key(), mask);
        return (start <= now && (cin <= start || cin > now)) ||
               (now < start && cin <= start && cin > now);
    }
    //     void skip(size_t &now, size_t mask)
    //     {
    //         Combined start = array[now];
    //         size_t end = now;
    //         auto off = start.off();
    //         auto key = start.ptr()->get_key();
    //         do {
    //             now = (now + 1) & mask;
    //             start = array[now];
    //         } while (start.valid() && start.off() == off &&
    //                  start.ptr()->get_key() == key && now != end);
    //     }
    //
    //     void _insert(size_t now, Combined com)
    //     {
    //         Combined other = array[now];
    //         array[now] = com;
    //         if (other.valid()) {
    //             _add(now, other.off(), other.ptr());
    //         } else {
    //             count++;
    //         }
    //     }
    //
    //     void _add(size_t now, size_t off, Data *data) {
    //         size_t mask = this->mask();
    //         auto key = std::ref(data->get_key());
    //         size_t border = 8 <= capacity ? 8 : capacity;
    //
    //         skip()
    //
    //         while(off<border){
    //             Combined other = array[now];
    //             if (other.valid()) {
    //                 _add(now, other.off(), other.ptr());
    //             } else {
    //                 _insert(now, RhHashMultiMap::Combined com)
    //             }
    //         }
    //
    //         increase_size();
    //         add(data);
    //     }

public:
    // // Inserts element with the given key.
    // void add(const K &key_in, D *data)
    // {
    //     assert(key_in == data->get_key());
    //
    //     if (count < capacity) {
    //         auto key = std::ref(key_in);
    //         size_t mask = this->mask();
    //         size_t now = index(key, mask);
    //         size_t off = 0;
    //
    //         bool bef_init = false;
    //         size_t before_off;
    //         auto before_key = std::ref(key);
    //         bool found_it = false;
    //
    //         size_t border = 8 <= capacity ? 8 : capacity;
    //         while (off < border) {
    //             Combined other = array[now];
    //             if (other.valid()) {
    //                 auto other_off = other.off();
    //                 auto other_key = std::ref<const
    //                 K>(other.ptr()->get_key());
    //                 if (other_off == off && key == other_key) {
    //                     found_it = true;
    //                     // Proceed
    //
    //                 } else if (other_off < off || found_it) { // Other is
    //                 rich
    //                                                           // or after
    //                                                           list
    //                                                           // of my keys
    //                     assert(other_off <= off);
    //
    //                     array[now] = Combined(data, off);
    //                     // add(other.ptr()->get_key(), other.ptr());
    //                     // return;
    //
    //                     // Hacked reusing of function
    //                     before_off = off;
    //                     before_key = key;
    //                     data = other.ptr();
    //                     key = other_key;
    //                     off = other_off;
    //
    //                     if (found_it) { // Offset isn't increased
    //                         found_it = false;
    //                     } else {
    //                         off++;
    //                     }
    //                 } else if (bef_init) { // Else other has equal or greater
    //                                        // offset, so he is poor.
    //                     if (before_off == other_off &&
    //                         before_key == other_key) {
    //                         if (count == capacity) {
    //                             break;
    //                         }
    //                         // Proceed
    //                     } else {
    //                         before_off = other_off;
    //                         before_key = other_key;
    //                         off++;
    //                     }
    //                 } else {
    //                     bef_init = true;
    //                     before_off = other_off;
    //                     before_key = other_key;
    //                     off++;
    //                 }
    //
    //             } else {
    //                 array[now] = Combined(data, off);
    //                 count++;
    //                 return;
    //             }
    //
    //             now = (now + 1) & mask;
    //         }
    //     }
    //
    //     increase_size();
    //     add(data);
    // }

    // Inserts element.
    void add(D *data) { add(data->get_key(), data); }

    // Removes element. Returns removed element if it existed. It doesn't
    // specify which element from same key group will be removed.
    OptionPtr<D> remove(const K &key_in)
    {
        // auto key = std::ref(key_in);
        // size_t mask = this->mask();
        // size_t now = index(key, mask);
        // size_t off = 0;
        //
        // bool bef_init = false;
        // size_t before_off;
        // auto before_key = key;
        // bool found_it = false;
        //
        // size_t border = 8 <= capacity ? 8 : capacity;
        // while (off < border) {
        //     Combined other = array[now];
        //     if (other.valid()) {
        //         auto other_off = other.off();
        //         auto other_ptr = other.ptr();
        //         auto other_key = std::ref<const K>(other_ptr->get_key());
        //         if (other_off == off && key == other_key) { // Found it
        //             found_it = true;
        //
        //         } else if (found_it) { // Found first element after last
        //         element
        //             // for remove.
        //             auto before = before_index(now, mask);
        //             auto ret = OptionPtr<D>(array[before].ptr());
        //             std::cout << "<-" << ret.get()->get_key() << "\n";
        //             while (other.valid() && other.off() > 0) {
        //                 std::cout << "<>" << other.ptr()->get_key() << "\n";
        //                 other.decrement_off();
        //                 array[before] = other;
        //                 before = now;
        //                 now = (now + 1) & mask;
        //                 other = array[now];
        //             }
        //
        //             array[before] = Combined();
        //             count--;
        //             return ret;
        //         } else if (other_off < off) { // Other is rich
        //             break;
        //
        //         } else if (bef_init) { // Else other has equal or greater
        //                                // offset, so he is poor.
        //             if (before_off == other_off && before_key == other_key) {
        //                 if (count == capacity) { // I am stuck.
        //                     break;
        //                 }
        //                 // Proceed
        //             } else {
        //                 before_off = other_off;
        //                 before_key = other_key;
        //                 off++;
        //             }
        //         } else {
        //             bef_init = true;
        //             before_off = other_off;
        //             before_key = other_key;
        //             off++;
        //         }
        //
        //     } else if (found_it) { // Found empty space after last element
        //     for
        //                            // remove.
        //         auto before = before_index(now, mask);
        //         auto ret = OptionPtr<D>(array[before].ptr());
        //         array[before] = Combined();
        //
        //         return ret;
        //     } else {
        //         break;
        //     }
        //
        //     now = (now + 1) & mask;
        // }
        return OptionPtr<D>();
    }

    void clear()
    {
        free(array);
        array = nullptr;
        capacity = 0;
        count = 0;
    }

    size_t size() const { return count; }

private:
    size_t before_index(size_t now, size_t mask)
    {
        return (now - 1) & mask; // THIS IS VALID
    }

    size_t index(const K &key, size_t mask) const
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

    size_t mask() const { return capacity - 1; }

    Combined *array = nullptr;
    size_t capacity = 0;
    size_t count = 0;

    friend class IteratorBase<Iterator>;
    friend class IteratorBase<ConstIterator>;
};
