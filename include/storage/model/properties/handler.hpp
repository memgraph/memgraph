#pragma once

#include "storage/model/properties/property.hpp"
#include "storage/model/properties/all.hpp"

template <class Handler>
void accept(Property &property, Handler &h)
{
    switch (property.flags) {

        case Property::Flags::True:
            return h.handle(static_cast<Bool &>(property));

        case Property::Flags::False:
            return h.handle(static_cast<Bool &>(property));

        case Property::Flags::String:
            return h.handle(static_cast<String &>(property));

        case Property::Flags::Int32:
            return h.handle(static_cast<Int32 &>(property));

        case Property::Flags::Int64:
            return h.handle(static_cast<Int64 &>(property));

        case Property::Flags::Float:
            return h.handle(static_cast<Float &>(property));

        case Property::Flags::Double:
            return h.handle(static_cast<Double &>(property));

        default:
            return;
    }
}
