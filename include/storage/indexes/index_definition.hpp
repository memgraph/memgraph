#pragma once

#include "utils/option.hpp"
#include "utils/order.hpp"
#include "utils/underlying_cast.hpp"

enum DbSide : uint8_t
{
    EdgeSide = 0,
    VertexSide = 1,
};

struct IndexType
{
public:
    // Are the records unique
    const bool unique;
    // Ordering of the records.
    const Order order;
};

// Defines location of index in a sense of what is necessary to be present in
// Edge/Vertex for it to be in index.
struct IndexLocation
{
public:
    // Returns code for location.
    size_t location_code() const
    {
        return (property_name.is_present() ? 1 : 0) |
               (label_name.is_present() ? 2 : 0) |
               (edge_type_name.is_present() ? 4 : 0);
    }

    IndexLocation clone() const
    {
        return IndexLocation{side, property_name, label_name, edge_type_name};
    }

    const DbSide side;
    const Option<std::string> property_name;
    const Option<std::string> label_name;
    const Option<std::string> edge_type_name;
};

// Fully answers:
// Index on what?
// What kind of index?
struct IndexDefinition
{
public:
    // Serializes self which can be deserialized.
    template <class E>
    void serialize(E &encoder) const
    {
        std::string empty;
        encoder.write_integer(underlying_cast(loc.side));
        encoder.write_string(loc.property_name.get_or(empty));
        encoder.write_string(loc.label_name.get_or(empty));
        encoder.write_string(loc.edge_type_name.get_or(empty));
        encoder.write_bool(type.unique);
        encoder.write_integer(underlying_cast(type.order));
    }

    // Deserializes self.
    template <class D>
    static IndexDefinition deserialize(D &decoder)
    {
        auto side = decoder.integer() == 0 ? EdgeSide : VertexSide;

        std::string property_name_s;
        decoder.string(property_name_s);
        auto property_name =
            property_name_s.empty()
                ? Option<std::string>()
                : Option<std::string>(std::move(property_name_s));

        std::string label_name_s;
        decoder.string(label_name_s);
        auto label_name = label_name_s.empty()
                              ? Option<std::string>()
                              : Option<std::string>(std::move(label_name_s));

        std::string edge_type_name_s;
        decoder.string(edge_type_name_s);
        auto edge_type_name =
            edge_type_name_s.empty()
                ? Option<std::string>()
                : Option<std::string>(std::move(edge_type_name_s));

        bool unique = decoder.read_bool();

        auto order_v = decoder.integer();
        auto order =
            order_v == 0 ? None : (order_v == 1 ? Ascending : Descending);

        return IndexDefinition{
            IndexLocation{side, property_name, label_name, edge_type_name},
            IndexType{unique, order}};
    }

    const IndexLocation loc;
    const IndexType type;
};
