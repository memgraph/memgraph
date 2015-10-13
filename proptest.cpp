#include <iostream>

#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/property.hpp"

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

int main(void)
{
    StringBuffer buffer;
    auto handler = JsonWriter<StringBuffer>(buffer);

    Properties props;
    props.emplace<Null>("sadness");
    props.emplace<Bool>("awesome", true);
    props.emplace<Bool>("lame", false);
    props.emplace<Int32>("age", 32);
    props.emplace<Int64>("money", 12345678910111213);
    props.emplace<String>("name", "caca");
    props.emplace<Float>("pi", 3.14159265358979323846264338327950288419716939937510582097);
    props.emplace<Double>("pi2", 3.141592653589793238462643383279502884197169399375105820);

    props.accept(handler);
    handler.finish();

    std::cout.precision(25);
    std::cout << buffer.str() << std::endl;

    return 0;
}
