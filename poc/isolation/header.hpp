#pragma once

// All fake types should be defined here as a interface.

#include <cassert>
#include <type_traits>

namespace sha
{

// Sized
class Name;
class Db;

// Unsized
class Accessor;

// Marks types which will be passed only be ref/pointer.
class Unsized
{
public:
    // This will assure that this class/derived classes can't be instantiated,
    // copyed or moved.
    // This way the other side can't "accidentaly" create/copy/move or destroy
    // this type because that would be errornus.
    Unsized() = delete;
    Unsized(const Unsized &other) = delete;
    Unsized(Unsized &&other) = delete;
    ~Unsized() = delete;
    Unsized &operator=(const Unsized &other) = delete;
    Unsized &operator=(Unsized &&other) = delete;

protected:
    template <class T>
    T &as()
    {
        return (*reinterpret_cast<T *>(this));
    }

    template <class T>
    const T &as() const
    {
        return (*reinterpret_cast<const T *>(this));
    }
};

// Every type which will be passed by value must extends this class.
template <std::size_t size_B, std::size_t alignment_B>
class Sized
{
public:
    // This will assure that this/derived class can't be instantiated.
    // This way if other side can't "accidentaly" create this/derived type
    // because that would be errornus.
    Sized() = delete;

    // This constructr also serves as a check for correctness of size and
    // aligment.
    Sized(std::size_t _size_B, std::size_t _alignment_B)
    {
        assert(size_B == _size_B);
        assert(alignment_B == _alignment_B);
    }

protected:
    template <class T>
    T &as()
    {
        return (*reinterpret_cast<T *>(&data));
    }

    template <class T>
    const T &as() const
    {
        return (*reinterpret_cast<const T *>(&data));
    }

private:
    // Here the first argument for template is size of struct in bytes. While
    // the second one is aligment of struct in bytes. Every class which will be
    // passed by value must have this kind of aligned storage with correct size
    // and aligment for that type. Unit tests to check this must be present.
    // Example would be:
    // std::aligned_storage<sizeof(std::set<int>), alignof(std::set<int>)>
    // While the resoults of sizeof and alignof would be passed as template
    // argumetns.
    // This values would be checked in tests like the following for example
    // above:
    // assert(sizeof(Accessor)==sizeof(std::set<int>));
    // assert(alignof(Accessor)==alignof(std::set<int>));
    std::aligned_storage<size_B, alignment_B> data;
};

// Type which will be passed by value so it's real size matters.
class Accessor : private Sized<16, 8>
{
public:
    // If the underlying type can't be copyed or moved this two constructors
    // would be deleted.
    Accessor(const Accessor &other);
    Accessor(Accessor &&other);

    // If the underlying type can't be copyed or moved this two operators
    // would be deleted.
    Accessor &operator=(const Accessor &other);
    Accessor &operator=(Accessor &&other);

    int get_prop(Name &name);
};

// Type which will be passed by ref/pointer only so it's size doesnt matter.
class Name : private Unsized
{
};

// Type which will be passed by ref/pointer only so it's size doesnt matter.
class Db : public Unsized
{
public:
    Accessor access();
    Name &get_name(const char *str);
};
}
