#include "query_engine/util.hpp"

#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"

template <class T>
void print_props(const Properties<T> &properties)
{
    StringBuffer buffer;
    JsonWriter<StringBuffer> writer(buffer);
    properties.accept(writer);
    cout << buffer.str() << endl;
}

template <class T>
void cout_properties(const Properties<T> &properties)
{
    ConsoleWriter writer;
    properties.accept(writer);
    cout << "----" << endl;
}

template <class T>
void cout_property(
    const typename PropertyFamily<T>::PropertyType::PropertyFamilyKey &key,
    const Property &property)
{
    ConsoleWriter writer;
    writer.handle<T>(key, property);
    cout << "----" << endl;
}

template void
print_props<TypeGroupEdge>(const Properties<TypeGroupEdge> &properties);

template void
print_props<TypeGroupVertex>(const Properties<TypeGroupVertex> &properties);

template void
cout_properties<TypeGroupEdge>(const Properties<TypeGroupEdge> &properties);

template void
cout_properties<TypeGroupVertex>(const Properties<TypeGroupVertex> &properties);

template void cout_property<TypeGroupEdge>(
    const typename PropertyFamily<
        TypeGroupEdge>::PropertyType::PropertyFamilyKey &key,
    const Property &property);

template void cout_property<TypeGroupVertex>(
    const typename PropertyFamily<
        TypeGroupVertex>::PropertyType::PropertyFamilyKey &key,
    const Property &property);
