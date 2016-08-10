#pragma once

#include "mvcc/record.hpp"
#include "storage/model/vertex_model.hpp"
#include "storage/model/properties/traversers/jsonwriter.hpp"

class Vertex : public mvcc::Record<Vertex>
{
public:
    class Accessor;

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
