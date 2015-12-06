#pragma once

#include "properties.hpp"

template <class Buffer>
struct JsonWriter
{
public:
    JsonWriter(Buffer& buffer) : buffer(buffer)
    {
        buffer << '{';
    };

    void handle(const std::string& key, Property& value, bool first)
    {
        if(!first)
            buffer << ',';

        buffer << '"' << key << "\":";
        value.accept(*this);
    }

    void handle(Null&)
    {
        buffer << "NULL";
    }

    void handle(Bool& b)
    {
        buffer << (b.value() ? "true" : "false");
    }

    void handle(String& s)
    {
        buffer << '"' << s.value << '"';
    }

    void handle(Int32& int32)
    {
        buffer << std::to_string(int32.value);
    }

    void handle(Int64& int64)
    {
        buffer << std::to_string(int64.value);
    }

    void handle(Float& f)
    {
        buffer << std::to_string(f.value);
    }

    void handle(Double& d)
    {
        buffer << std::to_string(d.value);
    }

    void finish()
    {
        buffer << '}';
    }

private:
    Buffer& buffer;
};

class StringBuffer
{
public:
    StringBuffer& operator<<(const std::string& str)
    {
        data += str;
        return *this;
    }

    StringBuffer& operator<<(const char* str)
    {
        data += str;
        return *this;
    }

    StringBuffer& operator<<(char c)
    {
        data += c;
        return *this;
    }

    std::string& str()
    {
        return data;
    }

private:
    std::string data;
};
