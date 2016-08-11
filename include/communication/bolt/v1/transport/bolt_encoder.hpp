#pragma once

#include <string>

#include "communication/bolt/v1/packing/codes.hpp"
#include "communication/bolt/v1/messaging/codes.hpp"
#include "utils/types/byte.hpp"
#include "utils/bswap.hpp"
#include "logging/default.hpp"

namespace bolt
{

template <class Stream>
class BoltEncoder
{
    static constexpr int64_t plus_2_to_the_31  =  2147483648L;
    static constexpr int64_t plus_2_to_the_15  =  32768L;
    static constexpr int64_t plus_2_to_the_7   =  128L;
    static constexpr int64_t minus_2_to_the_4  = -16L;
    static constexpr int64_t minus_2_to_the_7  = -128L;
    static constexpr int64_t minus_2_to_the_15 = -32768L;
    static constexpr int64_t minus_2_to_the_31 = -2147483648L;

public:
    BoltEncoder(Stream& stream) : stream(stream)
    {
        logger = logging::log->logger("Bolt Encoder");
    }

    void flush()
    {
        stream.flush();
    }

    void write(byte value)
    {
        write_byte(value);
    }

    void write_byte(byte value)
    {
        logger.trace("write byte: {}", value);
        stream.write(value);
    }

    void write(const byte* values, size_t n)
    {
        stream.write(values, n);
    }

    void write_null()
    {
        stream.write(pack::Null);
    }

    void write(bool value)
    {
        write_bool(value);
    }

    void write_bool(bool value)
    {
        if(value) write_true(); else write_false();
    }

    void write_true()
    {
        stream.write(pack::True);
    }

    void write_false()
    {
        stream.write(pack::False);
    }

    template <class T>
    void write_value(T value)
    {
        value = bswap(value);
        stream.write(reinterpret_cast<const byte*>(&value), sizeof(value));
    }

    void write_integer(int64_t value)
    {
        if(value >= minus_2_to_the_4 && value < plus_2_to_the_7)
        {
            write(static_cast<byte>(value));
        }
        else if(value >= minus_2_to_the_7 && value < minus_2_to_the_4)
        {
            write(pack::Int8);
            write(static_cast<byte>(value));
        }
        else if(value >= minus_2_to_the_15 && value < plus_2_to_the_15)
        {
            write(pack::Int16);
            write_value(static_cast<int16_t>(value));
        }
        else if(value >= minus_2_to_the_31 && value < plus_2_to_the_31)
        {
            write(pack::Int32);
            write_value(static_cast<int32_t>(value));
        }
        else
        {
            write(pack::Int64);
            write_value(value);
        }
    }

    void write(double value)
    {
        write_double(value);
    }

    void write_double(double value)
    {
        write(pack::Float64);
        write_value(*reinterpret_cast<const int64_t*>(&value));
    }

    void write_map_header(size_t size)
    {
        if(size < 0x10)
        {
            write(static_cast<byte>(pack::TinyMap | size));
        }
        else if(size <= 0xFF)
        {
            write(pack::Map8);
            write(static_cast<byte>(size));
        }
        else if(size <= 0xFFFF)
        {
            write(pack::Map16);
            write_value<uint16_t>(size);
        }
        else
        {
            write(pack::Map32);
            write_value<uint32_t>(size);
        }
    }

    void write_empty_map()
    {
        write(pack::TinyMap);
    }

    void write_list_header(size_t size)
    {
        if(size < 0x10)
        {
            write(static_cast<byte>(pack::TinyList | size));
        }
        else if(size <= 0xFF)
        {
            write(pack::List8);
            write(static_cast<byte>(size));
        }
        else if(size <= 0xFFFF)
        {
            write(pack::List16);
            write_value<uint16_t>(size);
        }
        else
        {
            write(pack::List32);
            write_value<uint32_t>(size);
        }
    }

    void write_empty_list()
    {
        write(pack::TinyList);
    }

    void write_string_header(size_t size)
    {
        if(size < 0x10)
        {
            write(static_cast<byte>(pack::TinyString | size));
        }
        else if(size <= 0xFF)
        {
            write(pack::String8);
            write(static_cast<byte>(size));
        }
        else if(size <= 0xFFFF)
        {
            write(pack::String16);
            write_value<uint16_t>(size);
        }
        else
        {
            write(pack::String32);
            write_value<uint32_t>(size);
        }
    }

    void write_string(const std::string& str)
    {
        write_string(str.c_str(), str.size());
    }

    void write_string(const char* str, size_t len)
    {
        write_string_header(len);
        write(reinterpret_cast<const byte*>(str), len);
    }

    void write_struct_header(size_t size)
    {
        if(size < 0x10)
        {
            write(static_cast<byte>(pack::TinyStruct | size));
        }
        else if(size <= 0xFF)
        {
            write(pack::Struct8);
            write(static_cast<byte>(size));
        }
        else
        {
            write(pack::Struct16);
            write_value<uint16_t>(size);
        }
    }

    void message_success()
    {
        write_struct_header(1);
        write(underlying_cast(MessageCode::Success));
    }

    void message_success_empty()
    {
        message_success();
        write_empty_map();
    }

    void message_record()
    {
        write_struct_header(1);
        write(underlying_cast(MessageCode::Record));
    }

    void message_record_empty()
    {
        message_record();
        write_empty_list();
    }

    void message_ignored()
    {
        write_struct_header(1);
        write(underlying_cast(MessageCode::Ignored));
    }

    void message_ignored_empty()
    {
        message_ignored();
        write_empty_map();
    }

protected:
    Logger logger;

private:
    Stream& stream;
};

}
