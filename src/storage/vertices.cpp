#include "storage/vertices.hpp"

Vertices::vertices_t::Accessor Vertices::access() { return vertices.access(); }

Option<const Vertex::Accessor> Vertices::find(DbTransaction &t, const Id &id)
{
    auto vertices_accessor = vertices.access();
    auto vertices_iterator = vertices_accessor.find(id);

    if (vertices_iterator == vertices_accessor.end())
        return make_option<const Vertex::Accessor>();

    return make_option_const(Vertex::Accessor(&vertices_iterator->second, t));
}

Vertex::Accessor Vertices::insert(DbTransaction &t)
{
    // get next vertex id
    auto next = counter.next();

    // create new vertex record
    VertexRecord vertex_record(next);
    // vertex_record.id(next);

    // insert the new vertex record into the vertex store
    auto vertices_accessor = vertices.access();
    auto result = vertices_accessor.insert(next, std::move(vertex_record));

    // create new vertex
    auto inserted_vertex_record = result.first;
    auto vertex = inserted_vertex_record->second.insert(t.trans);

    return Vertex::Accessor(vertex, &inserted_vertex_record->second, t);
}

void Vertices::update_label_index(const Label &label,
                                  VertexIndexRecord &&index_record)
{
    label_index.update(label, std::forward<VertexIndexRecord>(index_record));
}

VertexIndexRecordCollection &Vertices::find_label_index(const Label &label)
{
    return label_index.find(label);
}
