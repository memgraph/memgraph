#include <iostream>

#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/property.hpp"

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
