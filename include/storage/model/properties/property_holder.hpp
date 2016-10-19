#pragma once

#include "storage/model/properties/all.hpp"

// Generates constructor by value which accepts type_name object and stores them
// in union_name field.
#define GENERATE_CONSTRUCTOR_FOR_DATA(type_name, union_name)                   \
    PropertyHolder(type_name &&data, T k)                                      \
        : union_name(std::move(data)), key(k)                                  \
    {                                                                          \
        assert(type_name::type == key.get_type());                             \
    }

// Genrates case claus for Flags::type_name to construct type_name object in
// place of union_name field by a move from other
#define GENERATE_CASE_CLAUSE_FOR_CONSTRUCTOR_MOVE(type_name, union_name)       \
    case Flags::type_name: {                                                   \
        new (&(this->union_name)) type_name(std::move(other.union_name));      \
        break;                                                                 \
    }

// Genrates case claus for Flags::type_name to construct type_name object in
// place of union_name field by a copy from other
#define GENERATE_CASE_CLAUSE_FOR_CONSTRUCTOR_COPY(type_name, union_name)       \
    case Flags::type_name: {                                                   \
        new (&(this->union_name)) type_name(other.union_name);                 \
        break;                                                                 \
    }

// Genrates case claus for Flags::type_name to destruct type_name object in
// place of union_name field
#define GENERATE_CASE_CLAUSE_FOR_DESTRUCTOR(type_name, union_name)             \
    case Flags::type_name: {                                                   \
        this->union_name.~type_name();                                         \
        break;                                                                 \
    }

// Genrates case claus for Flags::type_name to handle type_name object from
// field of union_name
#define GENERATE_CASE_CLAUSE_FOR_HANDLER(type_name, union_name)                \
    case Flags::type_name: {                                                   \
        h.handle(this->union_name);                                            \
        break;                                                                 \
    }

// Genrates case claus for Flags::type_name to handle primitives of type_name
// object from field of union_name
#define GENERATE_CASE_CLAUSE_FOR_HANDLER_PRIMITIVE(type_name, union_name)      \
    case Flags::type_name: {                                                   \
        h.handle(this->union_name.value());                                    \
        break;                                                                 \
    }

// Genrates case claus for Flags::type_name to print type_name object from
// field of union_name
#define GENERATE_CASE_CLAUSE_FOR_PRINT(type_name, union_name)                  \
    case Flags::type_name: {                                                   \
        return this->union_name.print(stream);                                 \
    }

// Genrates case claus for Flags::type_name to comapre type_name object from
// field of union_name with same named and field from other holder.
#define GENERATE_CASE_CLAUSE_FOR_COMPARISON(type_name, union_name)             \
    case Flags::type_name: {                                                   \
        return this->union_name == other.union_name;                           \
    }

// Generates field in a union with a given type and name
#define GENERATE_UNION_FIELD(type_name, union_name) type_name union_name

// Generates signatures define_generator(type_name, union_name );
// for every pair type,property
#define GENERATE_FOR_ALL_PROPERTIES(define_generator)                           \
    define_generator(Null, null_v);                                            \
    define_generator(Bool, bool_v);                                            \
    define_generator(Int32, int32_v);                                          \
    define_generator(Int64, int64_v);                                          \
    define_generator(Float, float_V);                                          \
    define_generator(Double, double_v);                                        \
    define_generator(String, string_v);                                        \
    define_generator(ArrayBool, array_bool);                                   \
    define_generator(ArrayInt32, array_int32);                                 \
    define_generator(ArrayInt64, array_int64);                                 \
    define_generator(ArrayFloat, array_float);                                 \
    define_generator(ArrayDouble, array_double);                               \
    define_generator(ArrayString, array_string);

// Holds property and has some means of determining its type.
// T must have method get_type() const which returns Type.
template <class T>
class PropertyHolder
{
    // He is his own best friend.
    template <class O>
    friend class PropertyHolder;

public:
    PropertyHolder() = delete;

    GENERATE_FOR_ALL_PROPERTIES(GENERATE_CONSTRUCTOR_FOR_DATA);

    PropertyHolder(PropertyHolder const &other) : key(other.key)
    {
        switch (other.key.get_type().flags()) {
            GENERATE_FOR_ALL_PROPERTIES(
                GENERATE_CASE_CLAUSE_FOR_CONSTRUCTOR_COPY);
        default:
            assert(false);
        }
        assert(key.get_type().flags() == Flags::Int64 || this->int64_v != 0);
    }

    PropertyHolder(PropertyHolder &&other) : key(other.key)
    {
        switch (other.key.get_type().flags()) {
            GENERATE_FOR_ALL_PROPERTIES(
                GENERATE_CASE_CLAUSE_FOR_CONSTRUCTOR_MOVE);
        default:
            assert(false);
        }
    }

    template <class O>
    PropertyHolder(PropertyHolder<O> &&other, T const &key) : key(key)
    {
        assert(other.key.get_type() == key.get_type());
        switch (key.get_type().flags()) {
            GENERATE_FOR_ALL_PROPERTIES(
                GENERATE_CASE_CLAUSE_FOR_CONSTRUCTOR_MOVE);
        default:
            assert(false);
        }
        assert(key.get_type().flags() == Flags::Int64 || this->int64_v != 0);
    }

    ~PropertyHolder()
    {
        switch (key.get_type().flags()) {
            GENERATE_FOR_ALL_PROPERTIES(GENERATE_CASE_CLAUSE_FOR_DESTRUCTOR);
        default:
            assert(false);
        }
    }

    PropertyHolder &operator=(PropertyHolder const &other)
    {
        this->~PropertyHolder();
        new (this) PropertyHolder(other);
        assert(key.get_type().flags() == Flags::Int64 || this->int64_v != 0);
        return *this;
    }

    PropertyHolder &operator=(PropertyHolder &&other)
    {
        this->~PropertyHolder();
        new (this) PropertyHolder(std::move(other));
        assert(key.get_type().flags() == Flags::Int64 || this->int64_v != 0);
        return *this;
    }

    // Calls appropiate handler with a database property.
    // (String,Int64,ArrayBool,...)
    template <class Handler>
    void accept(Handler &h) const
    {
        switch (key.get_type().flags()) {
            GENERATE_FOR_ALL_PROPERTIES(GENERATE_CASE_CLAUSE_FOR_HANDLER);
        default:
            assert(false);
        }
    }

    // Calls appropiate handler with a primitive.(std::string,bool,int64_t,...)
    template <class Handler>
    void accept_primitive(Handler &h) const
    {
        switch (key.get_type().flags()) {
            GENERATE_FOR_ALL_PROPERTIES(
                GENERATE_CASE_CLAUSE_FOR_HANDLER_PRIMITIVE);
        default:
            assert(false);
        }
    }

    std::ostream &print(std::ostream &stream) const
    {
        switch (key.get_type().flags()) {
            GENERATE_FOR_ALL_PROPERTIES(GENERATE_CASE_CLAUSE_FOR_PRINT);
        default:
            // TODO: custom error
            throw std::runtime_error("PropertyHolder::print -> unknown flag");
        }
    }

    friend std::ostream &operator<<(std::ostream &stream,
                                    const PropertyHolder &prop)
    {
        return prop.print(stream);
    }

    bool operator==(const PropertyHolder &other) const
    {
        if (key == other.key) {
            switch (key.get_type().flags()) {
                GENERATE_FOR_ALL_PROPERTIES(GENERATE_CASE_CLAUSE_FOR_COMPARISON);
            default:
                assert(false);
                return false;
            }
        } else {
            return false;
        }
    }

    template <class O>
    bool operator==(const PropertyHolder<O> &other) const
    {
        if (key.get_type() == other.key.get_type()) {
            switch (key.get_type().flags()) {
                GENERATE_FOR_ALL_PROPERTIES(GENERATE_CASE_CLAUSE_FOR_COMPARISON);
            default:
                assert(false);
                return false;
            }
        } else {
            return false;
        }
    }

    template <class O, Flags f = O::type.flags()>
    bool operator==(const O &other) const
    {
        if (key.get_type() == O::type) {
            return other == as<O>();
        } else {
            return false;
        }
    }

    bool operator!=(const PropertyHolder &other) const
    {
        return !(*this == other);
    }

    // True if NULL
    bool is_empty() const { return is<Null>(); }

    // True if contains T property
    template <class D>
    bool is() const
    {
        return D::type == key.get_type();
    }

    // T MUST be type with which this holder was constructed.
    template <class D>
    D &as()
    {
        assert(is<D>());
        return *reinterpret_cast<D *>(&array_string);
    }

    // T MUST be type with which this PropertyData was constructed.
    // Specialized after this class
    template <class D>
    D const &as() const
    {
        assert(is<D>());
        return *reinterpret_cast<D const *>(&array_string);
    }

    // Knows type
    const T key;

private:
    // Stored data.
    union
    {
        GENERATE_FOR_ALL_PROPERTIES(GENERATE_UNION_FIELD);
    };
};

#undef GENERATE_CONSTRUCTOR_FOR_DATA
#undef GENERATE_CASE_CLAUSE_FOR_CONSTRUCTOR_MOVE
#undef GENERATE_CASE_CLAUSE_FOR_CONSTRUCTOR_COPY
#undef GENERATE_CASE_CLAUSE_FOR_DESTRUCTOR
#undef GENERATE_CASE_CLAUSE_FOR_HANDLER
#undef GENERATE_CASE_CLAUSE_FOR_PRINT
#undef GENERATE_CASE_CLAUSE_FOR_COMPARISON
#undef GENERATE_UNION_FIELD
#undef GENERATE_FOR_ALL_PROPERTIES
