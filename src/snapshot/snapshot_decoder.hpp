#pragma once

#include <fstream>
#include <unordered_map>

#include "communication/bolt/v1/transport/streamed_bolt_decoder.hpp"
#include "mvcc/id.hpp"
#include "serialization/graph_decoder.hpp"
#include "storage/indexes/index_definition.hpp"
#include "storage/model/properties/property.hpp"

// Decodes stored snapshot.
// Caller must respect loading order to be same as stored order with
// SnapshotEncoder.
// Main idea of knowing when something starts and ends is at certain points try
// to deserialize string and compare it with logically expected string seted by
// the SnapshotEncoder.
class SnapshotDecoder : public GraphDecoder
{
public:
    SnapshotDecoder(std::ifstream &snap_file);

    // Loads propert names, label names, edge_type names.
    void load_init();

    // Begins process of reading vertices
    void begin_vertices();

    // True if it is end of vertices
    bool end_vertices();

    // Begins process of loading edges
    void begin_edges();

    // True if it is end of edges
    bool end_edges();

    // Begins process of reading indexes.
    void start_indexes();

    // Loads IndexDefinition.
    IndexDefinition load_index();

    // True if it is end.
    bool end();

    // ***************** from GraphDecoder
    // Starts reading vertex.
    Id vertex_start();

    // Returns number of stored labels.
    size_t label_count();

    // Wiil read label into given storage.
    std::string const &label();

    // Starts reading edge. Return from to ids of connected vertices.
    std::pair<Id, Id> edge_start();

    // Reads edge_type into given storage.
    std::string const &edge_type();

    // Returns number of stored propertys.
    size_t property_count();

    // Reads property name into given storage.
    std::string const &property_name();

    // Reads property and calls T::handle for that property .
    template <class T>
    T property()
    {
        if (decoder.is_list()) {
            // Whe are deserializing an array.

            auto size = decoder.list_header();
            if (decoder.is_bool()) {
                ArrayStore<bool> store;
                for (auto i = 0; i < size; i++) {
                    store.push_back(decoder.read_bool());
                }
                return T::handle(std::move(store));

            } else if (decoder.is_integer()) {
                ArrayStore<int64_t> store;
                for (auto i = 0; i < size; i++) {
                    store.push_back(decoder.integer());
                }
                return T::handle(std::move(store));

            } else if (decoder.is_double()) {
                ArrayStore<double> store;
                for (auto i = 0; i < size; i++) {
                    store.push_back(decoder.read_double());
                }
                return T::handle(std::move(store));

            } else if (decoder.is_string()) {
                ArrayStore<std::string> store;
                for (auto i = 0; i < size; i++) {
                    std::string s;
                    decoder.string(s);
                    store.push_back(std::move(s));
                }
                return T::handle(std::move(store));
            }
        } else {
            // Whe are deserializing a primitive.

            if (decoder.is_bool()) {
                return T::handle(decoder.read_bool());

            } else if (decoder.is_integer()) {
                return T::handle(decoder.integer());

            } else if (decoder.is_double()) {
                return T::handle(decoder.read_double());

            } else if (decoder.is_string()) {
                std::string s;
                decoder.string(s);
                return T::handle(std::move(s));
            }
        }

        throw DecoderException("Tryed to read property but found "
                               "unknown type in bolt marked as: ",
                               decoder.mark());
    }

private:
    bolt::StreamedBoltDecoder<std::ifstream> decoder;

    // Contains for every property_name here snapshot local id.
    std::unordered_map<size_t, std::string> property_name_map;

    // Contains for every label_name here snapshot local id.
    std::unordered_map<size_t, std::string> label_name_map;

    // Contains for every edge_type here snapshot local id.
    std::unordered_map<size_t, std::string> edge_type_name_map;
};
