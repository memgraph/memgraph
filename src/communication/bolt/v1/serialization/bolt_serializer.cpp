#include "communication/bolt/v1/serialization/bolt_serializer.hpp"

#include "communication/bolt/v1/transport/chunked_buffer.hpp"
#include "communication/bolt/v1/transport/chunked_encoder.hpp"
#include "communication/bolt/v1/transport/socket_stream.hpp"
#include "storage/edge_x_vertex.hpp"

template <class Stream>
void bolt::BoltSerializer<Stream>::write(const EdgeAccessor &edge)
{
    // write signatures for the edge struct and edge data type
    encoder.write_struct_header(5);
    encoder.write(underlying_cast(pack::Relationship));

    // write the identifier for the node
    encoder.write_integer(edge.id());

    encoder.write_integer(edge.from().id());
    encoder.write_integer(edge.to().id());

    // write the type of the edge
    encoder.write_string(edge.edge_type());

    // write the property map
    auto props = edge.properties();

    encoder.write_map_header(props.size());

    for (auto &prop : props) {
        write(prop.first.family_name());
        write(*prop.second);
    }
}

// template class bolt::BoltSerializer<bolt::BoltEncoder<
//     bolt::ChunkedEncoder<bolt::ChunkedBuffer<bolt::SocketStream<io::Socket>>>>>;
