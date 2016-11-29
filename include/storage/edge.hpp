#pragma once

#include "mvcc/record.hpp"
#include "storage/model/edge_model.hpp"
#include "utils/string_buffer.hpp"
#include "storage/model/properties/json_writer.hpp"
#include "utils/handle_write.hpp"

class Edge : public mvcc::Record<Edge>
{
    using buffer_t        = utils::StringBuffer;
    using props_writer_t  = JsonWriter<buffer_t>;

public:
    class Accessor;

    Edge() = default;
    Edge(const EdgeModel &data) : data(data) {}
    Edge(EdgeModel &&data) : data(std::move(data)) {}

    Edge(const Edge &) = delete;
    Edge(Edge &&) = delete;

    Edge &operator=(const Edge &) = delete;
    Edge &operator=(Edge &&) = delete;

    EdgeModel data;

    template <typename Stream>
    void stream_repr(Stream &stream) const
    {
        auto props  = handle_write<buffer_t, props_writer_t>(data.props);

        stream << "Edge(cre = " << tx.cre() << ", "
               << "exp = " << tx.exp() << ", "
               << "props = " << props.str() << ")";
    }
};
