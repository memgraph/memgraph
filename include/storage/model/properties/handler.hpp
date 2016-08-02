#pragma once

#include "storage/model/properties/property.hpp"
#include "storage/model/properties/all.hpp"

template <class Handler>
void accept(const Property &property, Handler &h)
{
    switch (property.flags) {

        case Property::Flags::True:
            return h.handle(static_cast<const Bool &>(property));

        case Property::Flags::False:
            return h.handle(static_cast<const Bool &>(property));

        case Property::Flags::String:
            return h.handle(static_cast<const String &>(property));

        case Property::Flags::Int32:
            return h.handle(static_cast<const Int32 &>(property));

        case Property::Flags::Int64:
            return h.handle(static_cast<const Int64 &>(property));

        case Property::Flags::Float:
            return h.handle(static_cast<const Float &>(property));

        case Property::Flags::Double:
            return h.handle(static_cast<const Double &>(property));

        default:
            return;
    }
}
