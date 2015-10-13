#ifndef MEMGRAPH_STORAGE_VERTEX_HPP
#define MEMGRAPH_STORAGE_VERTEX_HPP

#include <vector>

#include "model/properties/jsonwriter.hpp"
#include "model/record.hpp"
#include "edge.hpp"

struct Vertex : public Record<Vertex>
{
    std::vector<Edge*> in;
    std::vector<Edge*> out;
};

inline std::ostream& operator<<(std::ostream& stream, Vertex& record)
{
    StringBuffer buffer;
    JsonWriter<StringBuffer> writer(buffer);

    // dump properties in this buffer
    record.properties.accept(writer);

    return stream << "Vertex" 
                  << "(xmin = " << record.tx.min()
                  << ", xmax = " << record.tx.max()
                  << "): " << buffer.str();
}
#endif
