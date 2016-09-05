#pragma once

#include <iostream>

#include "storage/model/properties/properties.hpp"
#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

using std::cout;
using std::endl;

class ConsoleWriter
{
public:
    ConsoleWriter() {}

    void handle(const StoredProperty<TypeGroupEdge> &value)
    {
        handle<TypeGroupEdge>(value);
    }

    void handle(const StoredProperty<TypeGroupVertex> &value)
    {
        handle<TypeGroupVertex>(value);
    }

    template <class T>
    void handle(StoredProperty<T> const &value)
    {
        cout << "KEY: " << value.key.family_name() << "; VALUE: ";

        value.accept(*this);

        // value.accept(*this);

        cout << endl;
    }

    void handle(const Null &v) { cout << "NULL"; }

    void handle(const Bool &b) { cout << b; }

    void handle(const String &s) { cout << s; }

    void handle(const Int32 &int32) { cout << int32; }

    void handle(const Int64 &int64) { cout << int64; }

    void handle(const Float &f) { cout << f; }

    void handle(const Double &d) { cout << d; }

    // Not yet implemented
    void handle(const ArrayBool &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayInt32 &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayInt64 &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayFloat &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayDouble &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayString &) { assert(false); }

    void finish() {}
};
