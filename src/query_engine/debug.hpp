#pragma once

#include <iostream>

#include "storage/model/properties/traversers/jsonwriter.hpp"
#include "storage/model/properties/properties.hpp"

using std::cout;
using std::endl;

void print_props(const Properties& properties)
{       
    StringBuffer buffer;
    JsonWriter<StringBuffer> writer(buffer);
    properties.accept(writer);
    cout << buffer.str() << endl;
}

#ifdef DEBUG
#   define PRINT_PROPS(_PROPS_) print_props(_PROPS_);
#else
#   define PRINT_PROPS(_)
#endif
