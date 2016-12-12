#pragma once

#include "mvcc/record.hpp"
#include "storage/label/labels_writer.hpp"
#include "storage/model/properties/json_writer.hpp"
#include "storage/model/vertex_model.hpp"
#include "utils/handle_write.hpp"
#include "utils/string_buffer.hpp"

class Vertex : public mvcc::Record<Vertex>
{
    using buffer_t        = utils::StringBuffer;
    using props_writer_t  = JsonWriter<buffer_t>;
    using labels_writer_t = LabelsWriter<buffer_t>;

public:
    class Accessor;

    Vertex() = default;
    Vertex(const VertexModel &data) : data(data) {}
    Vertex(VertexModel &&data) : data(std::move(data)) {}

    Vertex(const Vertex &) = delete;
    Vertex(Vertex &&)      = delete;

    Vertex &operator=(const Vertex &) = delete;
    Vertex &operator=(Vertex &&) = delete;

    VertexModel data;

    template <typename Stream>
    void stream_repr(Stream &stream) const
    {
        auto props  = handle_write<buffer_t, props_writer_t>(data.props);
        auto labels = handle_write<buffer_t, labels_writer_t>(data.labels);

        stream << "Vertex(cre = " << tx.cre() << ", "
               << "exp = " << tx.exp() << ", "
               << "props = " << props.str() << ", "
               << "labels = " << labels.str() << ")";
    }
};
