#pragma once

#include "storage/model/properties/all.hpp"
#include "storage/model/properties/property.hpp"

template <class Handler>
void accept(const Property &property, Handler &h)
{
    switch (property.flags) {

    case Flags::True:
        return h.handle(static_cast<const Bool &>(property));

    case Flags::False:
        return h.handle(static_cast<const Bool &>(property));

    case Flags::String:
        return h.handle(static_cast<const String &>(property));

    case Flags::Int32:
        return h.handle(static_cast<const Int32 &>(property));

    case Flags::Int64:
        return h.handle(static_cast<const Int64 &>(property));

    case Flags::Float:
        return h.handle(static_cast<const Float &>(property));

    case Flags::Double:
        return h.handle(static_cast<const Double &>(property));

    default:
        return;
    }
}
