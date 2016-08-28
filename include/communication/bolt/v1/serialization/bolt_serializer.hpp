#pragma once

#include "communication/bolt/v1/packing/codes.hpp"
#include "communication/bolt/v1/transport/bolt_encoder.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/edge_x_vertex.hpp"
#include "storage/vertex_accessor.hpp"

#include "storage/model/properties/all.hpp"
#include "storage/model/properties/properties.hpp"

namespace bolt
{

template <class Stream>
class BoltSerializer
{
    friend class Property;

    // TODO: here shoud be friend but it doesn't work
    // template <class Handler>
    // friend void accept(const Property &property, Handler &h);

public:
    BoltSerializer(Stream &stream) : encoder(stream) {}

    /* Serializes the vertex accessor into the packstream format
     *
     * struct[size = 3] Vertex [signature = 0x4E] {
     *     Integer            node_id;
     *     List<String>       labels;
     *     Map<String, Value> properties;
     * }
     *
     */
    void write(const VertexAccessor &vertex)
    {
        // write signatures for the node struct and node data type
        encoder.write_struct_header(3);
        encoder.write(underlying_cast(pack::Node));

        // write the identifier for the node
        encoder.write_integer(vertex.id());

        // write the list of labels
        auto labels = vertex.labels();

        encoder.write_list_header(labels.size());

        for (auto &label : labels)
            encoder.write_string(label.get());

        // write the property map
        auto props = vertex.properties();

        encoder.write_map_header(props.size());

        for (auto &prop : props) {
            write(prop.first.family_name());
            write(*prop.second);
        }
    }

    /* Serializes the vertex accessor into the packstream format
     *
     * struct[size = 5] Edge [signature = 0x52] {
     *     Integer            edge_id;
     *     Integer            start_node_id;
     *     Integer            end_node_id;
     *     String             type;
     *     Map<String, Value> properties;
     * }
     *
     */
    void write(const EdgeAccessor &edge)
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

    void write(const Property &prop) { accept(prop, *this); }

    void write_null() { encoder.write_null(); }

    void write(const Bool &prop) { encoder.write_bool(prop.value()); }

    void write(const Float &prop) { encoder.write_double(prop.value); }

    void write(const Double &prop) { encoder.write_double(prop.value); }

    void write(const Int32 &prop) { encoder.write_integer(prop.value); }

    void write(const Int64 &prop) { encoder.write_integer(prop.value); }

    void write(const std::string &value) { encoder.write_string(value); }

    void write(const String &prop) { encoder.write_string(prop.value); }

    template <class T>
    void handle(const T &prop)
    {
        write(prop);
    }

protected:
    Stream &encoder;
};
}
