#include "query_engine/util.hpp"

void print_props(const Properties &properties)
{
    StringBuffer buffer;
    JsonWriter<StringBuffer> writer(buffer);
    properties.accept(writer);
    cout << buffer.str() << endl;
}

void cout_properties(const Properties &properties)
{
    ConsoleWriter writer;
    properties.accept(writer);
    cout << "----" << endl;
}

void cout_property(const prop_key_t &key, const Property &property)
{
    ConsoleWriter writer;
    writer.handle(key, property);
    cout << "----" << endl;
}
