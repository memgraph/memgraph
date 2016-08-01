#pragma once

#include <cstdint>

namespace bolt
{

namespace pack
{

enum Code : uint8_t
{
    TinyString  = 0x80,
    TinyList    = 0x90,
    TinyMap     = 0xA0,
    TinyStruct  = 0xB0,

    Null        = 0xC0,

    Float64     = 0xC1,

    False       = 0xC2,
    True        = 0xC3,

    Int8        = 0xC8,
    Int16       = 0xC9,
    Int32       = 0xCA,
    Int64       = 0xCB,

    Bytes8      = 0xCC,
    Bytes16     = 0xCD,
    Bytes32     = 0xCE,

    String8     = 0xD0,
    String16    = 0xD1,
    String32    = 0xD2,

    List8       = 0xD4,
    List16      = 0xD5,
    List32      = 0xD6,

    Map8        = 0xD8,
    Map16       = 0xD9,
    Map32       = 0xDA,
    MapStream   = 0xDB,

    Struct8     = 0xDC,
    Struct16    = 0xDD,
    EndOfStream = 0xDF,
};

}

}
