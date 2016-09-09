#include "snapshot/snapshot_decoder.hpp"

SnapshotDecoder::SnapshotDecoder(std::ifstream &snap_file) : decoder(snap_file)
{
}

// Loads propert names, label names, edge_type names.
void SnapshotDecoder::load_init()
{
    for (auto i = decoder.map_header(); i > 0; i--) {
        std::string name;
        decoder.string(name);
        auto id = decoder.integer();
        property_name_map.insert(std::make_pair(id, std::move(name)));
    }

    for (auto i = decoder.map_header(); i > 0; i--) {
        std::string name;
        decoder.string(name);
        auto id = decoder.integer();
        label_name_map.insert(std::make_pair(id, std::move(name)));
    }

    for (auto i = decoder.map_header(); i > 0; i--) {
        std::string name;
        decoder.string(name);
        auto id = decoder.integer();
        edge_type_name_map.insert(std::make_pair(id, std::move(name)));
    }
}

// Begins process of reading vertices
void SnapshotDecoder::begin_vertices()
{
    std::string name;
    decoder.string(name);
    if (name != "vertices") {
        throw DecoderException(
            "Tryed to start reading vertices on illegal position marked as: " +
            name);
    }
}

// True if it is end of vertices
bool SnapshotDecoder::end_vertices()
{
    std::string name;
    bool ret = decoder.string_try(name);
    if (ret && name != "edges") {
        throw DecoderException(
            "Tryed to end reading vertices on illegal position marked as: " +
            name);
    }
    return ret;
}

// Begins process of loading edges
void SnapshotDecoder::begin_edges()
{
    // EMPTY
}

// True if it is end of edges
bool SnapshotDecoder::end_edges()
{
    std::string name;
    bool ret = decoder.string_try(name);
    if (ret && name != "indexes") {
        throw DecoderException(
            "Tryed to end reading edges on illegal position marked as: " +
            name);
    }
    return ret;
}

// Begins process of reading indexes.
void SnapshotDecoder::start_indexes()
{
    // EMPTY
}

// Loads IndexDefinition.
IndexDefinition SnapshotDecoder::load_index()
{
    return IndexDefinition::deserialize(decoder);
}

// True if it is end.
bool SnapshotDecoder::end()
{
    std::string name;
    bool ret = decoder.string_try(name);
    if (ret && name != "end") {
        throw DecoderException("Tryed to end on illegal position marked as: " +
                               name);
    }
    return ret;
}

// ***************** from GraphDecoder
// Starts reading vertex.
Id SnapshotDecoder::vertex_start() { return Id(decoder.integer()); }

// Returns number of stored labels.
size_t SnapshotDecoder::label_count() { return decoder.list_header(); }

// Wiil read label into given storage.
std::string const &SnapshotDecoder::label()
{
    return label_name_map.at(decoder.integer());
}

// Starts reading edge. Return from to ids of connected vertices.
std::pair<Id, Id> SnapshotDecoder::edge_start()
{
    auto from = Id(decoder.integer());
    auto to = Id(decoder.integer());
    return std::make_pair(from, to);
}

// Reads edge_type into given storage.
std::string const &SnapshotDecoder::edge_type()
{
    return edge_type_name_map.at(decoder.integer());
}

// Returns number of stored propertys.
size_t SnapshotDecoder::property_count() { return decoder.map_header(); }

// Reads property name into given storage.
std::string const &SnapshotDecoder::property_name()
{
    return property_name_map.at(decoder.integer());
}
