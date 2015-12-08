#pragma once

#include "model/properties/jsonwriter.hpp"
#include "model/vertex_model.hpp"
#include "mvcc/record.hpp"

class Vertex : public mvcc::Record<Vertex>
{
public:
    Vertex() = default;
    Vertex(const VertexModel& data) : data(data) {}
    Vertex(VertexModel&& data) : data(std::move(data)) {}

    Vertex(const Vertex&) = delete;
    Vertex(Vertex&&) = delete;

    Vertex& operator=(const Vertex&) = delete;
    Vertex& operator=(Vertex&&) = delete;

    VertexModel data;
};

inline std::ostream& operator<<(std::ostream& stream, const Vertex& record)
{
    StringBuffer buffer;
    JsonWriter<StringBuffer> writer(buffer);

    // dump properties in this buffer
    record.data.props.accept(writer);
    writer.finish();

    return stream << "Vertex"
                  << "(cre = " << record.tx.cre()
                  << ", exp = " << record.tx.exp()
                  << "): " << buffer.str();
}

// TODO: find more appropriate place for this
inline std::string properties_to_string(const Vertex* vertex)
{
    // make a string buffer
    StringBuffer buffer;
    JsonWriter<StringBuffer> writer(buffer);

    // dump properties in this buffer
    vertex->data.props.accept(writer);
    writer.finish();

    // respond to the use with the buffer
    return std::move(buffer.str());
}
