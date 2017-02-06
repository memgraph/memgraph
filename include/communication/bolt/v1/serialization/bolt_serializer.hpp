#pragma once

#include "communication/bolt/v1/packing/codes.hpp"
#include "communication/bolt/v1/transport/bolt_encoder.hpp"

#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

#include "storage/edge_type/edge_type.hpp"
#include "storage/label/label.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/model/properties/properties.hpp"
#include "storage/vertex_record.hpp"

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

    /** Serializes the vertex accessor into the packstream format
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

        // IMPORTANT: here we write a hardcorded 0 because we don't
        // use internal IDs, but need to give something to Bolt
        // note that OpenCypther has no id(x) function, so the client
        // should not be able to do anything with this value anyway
        encoder.write_integer(0);

        // write the list of labels
        auto labels = vertex.labels();

        encoder.write_list_header(labels.size());

        for (auto &label : labels)
            encoder.write_string(label.get());

        // write the property map
        auto props = vertex.properties();

        encoder.write_map_header(props.size());

        for (auto &prop : props) {
            write(prop.key.family_name());
            prop.accept(*this);
        }
    }

    /** Serializes the vertex accessor into the packstream format
     *
     * struct[size = 5] Edge [signature = 0x52] {
     *     Integer            edge_id;          // IMPORTANT: always 0 since we don't do IDs
     *     Integer            start_node_id;    // IMPORTANT: always 0 since we don't do IDs
     *     Integer            end_node_id;      // IMPORTANT: always 0 since we don't do IDs
     *     String             type;
     *     Map<String, Value> properties;
     * }
     *
     */
    void write(const EdgeAccessor &edge);

    void write_null() { encoder.write_null(); }

    void write(const Null &) { encoder.write_null(); }

    void write(const Bool &prop) { encoder.write_bool(prop.value()); }

    void write(const Float &prop) { encoder.write_double(prop.value()); }

    void write(const Double &prop) { encoder.write_double(prop.value()); }

    void write(const Int32 &prop) { encoder.write_integer(prop.value()); }

    void write(const Int64 &prop) { encoder.write_integer(prop.value()); }

    void write(const String &value) { encoder.write_string(value.value()); }

    // Not yet implemented
    void write(const ArrayBool &) { assert(false); }

    // Not yet implemented
    void write(const ArrayInt32 &) { assert(false); }

    // Not yet implemented
    void write(const ArrayInt64 &) { assert(false); }

    // Not yet implemented
    void write(const ArrayFloat &) { assert(false); }

    // Not yet implemented
    void write(const ArrayDouble &) { assert(false); }

    // Not yet implemented
    void write(const ArrayString &) { assert(false); }

    void write_failure(const std::map<std::string, std::string> &data)
    {
        encoder.message_failure();
        encoder.write_map_header(data.size());
        for (auto const &kv : data) {
            write(kv.first);
            write(kv.second);
        }
    }

    template <class T>
    void handle(const T &prop)
    {
        write(prop);
    }

protected:
    Stream &encoder;
};
}
