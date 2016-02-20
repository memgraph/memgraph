#include <iostream>

#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/property.hpp"
#include "storage/model/properties/traversers/jsonwriter.hpp"

using std::endl;
using std::cout;

int main(void)
{
    Properties props;
    props.set<Bool>("awesome", true);
    props.set<Bool>("lame", false);
    props.set<Int32>("age", 32);

    // integral
    Int32 a = 12;
    Int32 b = 24;
    Int32 c = b;

    Property& d = b;

    cout << "a = " << a << "; b = " << b << endl;
    cout << (a > b) << (a < b) << (a == b) << (a != b) << endl;

    cout << "b == d" << " -> " << (b == d) << endl;

    Float x = 3.14;
    Float y = 6.28;

    Float z = x * 3.28 / y + a * b + 3;

    cout << x << endl;
    cout << z << endl;

    props.set<Float>("pi", z);

    StringBuffer buffer;
    JsonWriter<StringBuffer> writer(buffer);

    props.accept(writer);
    cout << buffer.str() << endl;

    return 0;
}



