#ifndef MEMGRAPH_STORAGE_VERTEX_HPP
#define MEMGRAPH_STORAGE_VERTEX_HPP

#include <vector>

#include "model/record.hpp"
#include "edge.hpp"

struct Vertex : public Record<Vertex>
{
    std::vector<Edge*> in;
    std::vector<Edge*> out;
};

inline std::ostream& operator<<(std::ostream& stream, Vertex& record)
{
    std::string props;
    record.properties.dump(props);

    return stream << "Vertex" 
                  << "(xmin = " << record.tx.min()
                  << ", xmax = " << record.tx.max()
                  << "): " << props;
}
#endif
