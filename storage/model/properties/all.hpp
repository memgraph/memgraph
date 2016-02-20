#pragma once

#include "property.hpp"

#include "null.hpp"
#include "bool.hpp"
#include "string.hpp"
#include "int32.hpp"
#include "int64.hpp"
#include "float.hpp"
#include "double.hpp"

template <class Handler>
void Property::accept(Handler& h)
{
    switch(flags)
    {
        case Flags::True:   return h.handle(static_cast<Bool&>(*this));
        case Flags::False:  return h.handle(static_cast<Bool&>(*this));
        case Flags::String: return h.handle(static_cast<String&>(*this));
        case Flags::Int32:  return h.handle(static_cast<Int32&>(*this));
        case Flags::Int64:  return h.handle(static_cast<Int64&>(*this));
        case Flags::Float:  return h.handle(static_cast<Float&>(*this));
        case Flags::Double: return h.handle(static_cast<Double&>(*this));
        default: return;
    }
}
