#pragma once

#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/handler.hpp"

template <class Buffer>
struct JsonWriter
{
public:
    JsonWriter(Buffer &buffer) : buffer(buffer) { buffer << '{'; };

    void handle(const std::string &key, const Property &value)
    {
        if (!first) buffer << ',';

        if (first) first = false;

        buffer << '"' << key << "\":";
        // value.accept(*this);
        accept(value, *this);
    }

    void handle(const Bool &b) { buffer << (b.value() ? "true" : "false"); }

    void handle(const String &s) { buffer << '"' << s.value << '"'; }

    void handle(const Int32 &int32) { buffer << std::to_string(int32.value); }

    void handle(const Int64 &int64) { buffer << std::to_string(int64.value); }

    void handle(const Float &f) { buffer << std::to_string(f.value); }

    void handle(const Double &d) { buffer << std::to_string(d.value); }

    void finish() { buffer << '}'; }

private:
    bool first{true};
    Buffer &buffer;
};

class StringBuffer
{
public:
    StringBuffer &operator<<(const std::string &str)
    {
        data += str;
        return *this;
    }

    StringBuffer &operator<<(const char *str)
    {
        data += str;
        return *this;
    }

    StringBuffer &operator<<(char c)
    {
        data += c;
        return *this;
    }

    std::string &str() { return data; }

private:
    std::string data;
};
