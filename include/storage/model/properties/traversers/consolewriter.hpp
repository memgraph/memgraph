#pragma once

#include <iostream>

#include "storage/model/properties/handler.hpp"
#include "storage/model/properties/properties.hpp"

using std::cout;
using std::endl;

class ConsoleWriter
{
public:
    ConsoleWriter() {}

    void handle(const prop_key_t &key, const Property &value)
    {
        cout << "KEY: " << key.family_name() << "; VALUE: ";

        accept(value, *this);

        // value.accept(*this);

        cout << endl;
    }

    void handle(const Bool &b) { cout << b.value(); }

    void handle(const String &s) { cout << s.value; }

    void handle(const Int32 &int32) { cout << int32.value; }

    void handle(const Int64 &int64) { cout << int64.value; }

    void handle(const Float &f) { cout << f.value; }

    void handle(const Double &d) { cout << d.value; }

    void finish() {}
};
