#pragma once

namespace bolt
{

enum class PackType
{
    Null,        // denotes absence of a value
    Boolean,     // denotes a type with two possible values (t/f)
    Integer,     // 64-bit signed integral number
    Float,       // 64-bit floating point number
    Bytes,       // binary data
    String,      // unicode string
    List,        // collection of values
    Map,         // collection of zero or more key/value pairs
    Struct,      // zero or more packstream values
    EndOfStream, // denotes stream value end
    Reserved     // reserved for future use
};

}
