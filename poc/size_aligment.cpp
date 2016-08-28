#include "database/db.hpp"
#include "database/db_accessor.hpp"

#include <chrono>
#include <ctime>
#include <strings.h>
#include <unistd.h>
#include <unordered_map>
#include "database/db_accessor.cpp"
#include "import/csv_import.hpp"
#include "storage/edge_x_vertex.hpp"
#include "storage/indexes/impl/nonunique_unordered_index.cpp"
#include "storage/model/properties/properties.cpp"
// #include "storage/record_accessor.cpp"
// #include "storage/vertex_accessor.cpp"
#include "utils/command_line/arguments.hpp"

using namespace std;

using vertex_access_iterator =
    decltype(((DbAccessor *)nullptr_t())->vertex_access());

using out_edge_iterator_t =
    decltype(((VertexAccessor *)(std::nullptr_t()))->out());

using in_edge_iterator_t =
    decltype(((::VertexAccessor *)(std::nullptr_t()))->in());

int main()
{
    cout << "DbAccessor.vertex_access(): size: "
         << sizeof(vertex_access_iterator)
         << " aligment: " << alignof(vertex_access_iterator) << endl;

    cout << "DbAccessor: size: " << sizeof(DbAccessor)
         << " aligment: " << alignof(DbAccessor) << endl;

    cout << "VertexAccessor: size: " << sizeof(VertexAccessor)
         << " aligment: " << alignof(VertexAccessor) << endl;

    cout << "std::unique_ptr<IteratorBase<const ::VertexAccessor>>: size: "
         << sizeof(std::unique_ptr<IteratorBase<const ::VertexAccessor>>)
         << " aligment: "
         << alignof(std::unique_ptr<IteratorBase<const ::VertexAccessor>>)
         << endl;

    cout << "VertexAccessor.out(): size: " << sizeof(out_edge_iterator_t)
         << " aligment: " << alignof(out_edge_iterator_t) << endl;

    cout << "VertexAccessor.in(): size: " << sizeof(in_edge_iterator_t)
         << " aligment: " << alignof(in_edge_iterator_t) << endl;

    // cout << ": size: " << sizeof(void) << " aligment: " << alignof(void)
    //      << endl;

    return 0;
}
