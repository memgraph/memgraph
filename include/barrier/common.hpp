#pragma once

#include <cassert>
#include <type_traits>
#include <utility>
#include <vector>

// THis shoul be the only place to include code from memgraph other than
// barrier.cpp
#include "mvcc/id.hpp"
#include "storage/model/properties/all.hpp"
#include "utils/border.hpp"
#include "utils/iterator/iterator.hpp"
#include "utils/option_ptr.hpp"
#include "utils/order.hpp"
#include "utils/reference_wrapper.hpp"

// Contains common classes and functions for barrier.hpp and barrier.cpp.
namespace barrier
{

// This define accepts other define which accepts property type to instantiate
// template. Recieved define will be called for all property types.
#define INSTANTIATE_FOR_PROPERTY(x)                                            \
    x(Int32) x(Int64) x(Float) x(Double) x(Bool) x(String) x(ArrayInt32)       \
        x(ArrayInt64) x(ArrayFloat) x(ArrayDouble) x(ArrayBool) x(ArrayString)

// **************************** HELPER FUNCTIONS **************************** //
template <class TO, class FROM>
TO &ref_as(FROM &ref)
{
    return (*reinterpret_cast<TO *>(&ref));
}

template <class TO, class FROM>
TO const &ref_as(FROM const &ref)
{
    return (*reinterpret_cast<TO const *>(&ref));
}

template <class TO, class FROM>
TO *ptr_as(FROM *ref)
{
    return (reinterpret_cast<TO *>(ref));
}

template <class TO, class FROM>
TO const *ptr_as(FROM const *ref)
{
    return (reinterpret_cast<TO const *>(ref));
}

template <class TO, class FROM>
TO &&value_as(FROM &&ref)
{
    static_assert(sizeof(TO) == sizeof(FROM), "Border class size mismatch");
    static_assert(alignof(TO) == alignof(FROM),
                  "Border class aligment mismatch");
    return (reinterpret_cast<TO &&>(std::move(ref)));
}

template <class TO, class FROM>
const TO &&value_as(const FROM &&ref)
{
    static_assert(sizeof(TO) == sizeof(FROM), "Border class size mismatch");
    static_assert(alignof(TO) == alignof(FROM),
                  "Border class aligment mismatch");
    return (reinterpret_cast<TO const &&>(std::move(ref)));
}

// Barrier classes which will be used only through reference/pointer should
// inherit this class.
class Unsized
{
public:
    // Deleting following constructors/destroyers and opertators  will assure
    // that this class/derived classes can't be created,copyed,deleted or moved.
    // This way the other side can't "accidentaly" create/copy/destroy or move
    // objects which inherit this class because that would be erroneous.
    Unsized() = delete;
    Unsized(const Unsized &other) = delete;
    Unsized(Unsized &&other) = delete;
    ~Unsized() = delete;
    Unsized &operator=(const Unsized &other) = delete;
    Unsized &operator=(Unsized &&other) = delete;
};

// Barrier classes which will be used as value should inherit this class.
// Template accepts size_B in B of object of original class from memgraph.
// Template accepts alignment_B in B of object of original class from memgraph.
template <std::size_t size_B, std::size_t alignment_B>
class Sized
{
protected:
    // This will ensure that this/derived class can't be instantiated.
    // This way side outside the barrier can't "accidentaly" create this/derived
    // type because that would be erroneous.
    Sized() = delete;

    // This constructor serves as a check for correctness of size_B and
    // alignment_B template parametars. Derived class MUST call this constructor
    // with T which it holds where T is original class from memgraph.
    template <class T>
    Sized(T &&d)
    {
        new (ptr_as<T>(&data)) T(std::move(d));
        static_assert(size_B == sizeof(T), "Border class size mismatch");
        static_assert(alignment_B == alignof(T),
                      "Border class aligment mismatch");
    }

    // This constructor serves as a check for correctness of size_B and
    // alignment_B template parametars. Derived class MUST call this constructor
    // with T which it holds where T is original class from memgraph.
    template <class T>
    Sized(const T &&d)
    {
        new (ptr_as<T>(&data)) T(std::move(d));
        static_assert(size_B == sizeof(T), "Border class size mismatch");
        static_assert(alignment_B == alignof(T),
                      "Border class aligment mismatch");
    }

private:
    // Here is the aligned storage which imitates size and aligment of object of
    // original class from memgraph.
    typename std::aligned_storage<size_B, alignment_B>::type data;
};

// HELPER FUNCTIONS
template <class R>
bool option_fill(Option<R> &o)
{
    return o.is_present() && o.get().fill();
}
}
