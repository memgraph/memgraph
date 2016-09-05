#pragma once

#include "storage/model/properties/properties.hpp"
#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

template <class Buffer>
struct JsonWriter
{
public:
    JsonWriter(Buffer &buffer) : buffer(buffer) { buffer << '{'; };

    void handle(const StoredProperty<TypeGroupEdge> &prop)
    {
        handle<TypeGroupEdge>(prop);
    }

    void handle(const StoredProperty<TypeGroupVertex> &prop)
    {
        handle<TypeGroupVertex>(prop);
    }

    template <class TG>
    void handle(const StoredProperty<TG> &prop)
    {
        if (!first) buffer << ',';

        if (first) first = false;

        buffer << '"' << prop.get_property_key().family_name() << "\":";

        prop.accept(*this);
    }

    void handle(const Null &v) { buffer << "NULL"; }

    void handle(const Bool &b) { buffer << (b ? "true" : "false"); }

    void handle(const String &s) { buffer << '"' << s.value() << '"'; }

    void handle(const Int32 &int32) { buffer << std::to_string(int32.value()); }

    void handle(const Int64 &int64) { buffer << std::to_string(int64.value()); }

    void handle(const Float &f) { buffer << std::to_string(f.value()); }

    void handle(const Double &d) { buffer << std::to_string(d.value()); }

    // Not yet implemented
    void handle(const ArrayBool &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayInt32 &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayInt64 &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayFloat &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayDouble &) { assert(false); }

    // Not yet implemented
    void handle(const ArrayString &) { assert(false); }

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
