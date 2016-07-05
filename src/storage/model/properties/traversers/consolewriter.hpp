#pragma once

#include <iostream>

#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/handler.hpp"

using std::cout;
using std::endl;

class ConsoleWriter
{
public:
    ConsoleWriter() {}

    void handle(const std::string &key, Property &value)
    {
        cout << "KEY: " << key << "; VALUE: ";

        accept(value, *this);
    
        // value.accept(*this);

        cout << endl;
    }

    void handle(Bool &b) { cout << b.value(); }

    void handle(String &s) { cout << s.value; }

    void handle(Int32 &int32) { cout << int32.value; }

    void handle(Int64 &int64) { cout << int64.value; }

    void handle(Float &f) { cout << f.value; }

    void handle(Double &d) { cout << d.value; }

    void finish() {}
};
