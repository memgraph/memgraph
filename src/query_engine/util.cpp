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
void cout_property(const StoredProperty<T> &property)
{
    ConsoleWriter writer;
    property.accept(writer);
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

template void
cout_property<TypeGroupEdge>(const StoredProperty<TypeGroupEdge> &property);

template void
cout_property<TypeGroupVertex>(const StoredProperty<TypeGroupVertex> &property);
