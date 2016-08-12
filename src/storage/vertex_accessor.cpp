#include "database/db.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/vertices.hpp"

size_t Vertex::Accessor::out_degree() const
{
    return this->record->data.out.degree();
}

size_t Vertex::Accessor::in_degree() const
{
    return this->record->data.in.degree();
}

size_t Vertex::Accessor::degree() const { return in_degree() + out_degree(); }

void Vertex::Accessor::add_label(const Label &label)
{
    // update vertex
    this->record->data.labels.add(label);

    // update index
    this->db.update_label_index(label,
                                VertexIndexRecord(this->record, this->vlist));
}

bool Vertex::Accessor::has_label(const Label &label) const
{
    return this->record->data.labels.has(label);
}

const std::set<label_ref_t> &Vertex::Accessor::labels() const
{
    return this->record->data.labels();
}
