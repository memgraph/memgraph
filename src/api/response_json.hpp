#pragma once

#include "storage/vertex.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/writer/rapidjson_stringwriter.hpp"

StringBuffer vertex_props_to_buffer(const Vertex* vertex)
{
    // make a string buffer
    StringBuffer buffer;
    JsonWriter<StringBuffer> writer(buffer);

    // dump properties in this buffer
    vertex->data.props.accept(writer);
    writer.finish();

    // respond to the use with the buffer
    return buffer;
}

std::string vertex_props_to_string(const Vertex* vertex)
{
    auto buffer = vertex_props_to_buffer(vertex);
    return std::move(buffer.str());
}

// TODO: clear up naming
using RJStringBuffer = rapidjson::StringBuffer;
using RJStringWriter = rapidjson::Writer<RJStringBuffer>;
using ptr_RJStringWriter = std::shared_ptr<RJStringWriter>;

std::string vertex_create_response(const VertexAccessor& vertex_accessor)
{
    // make a string buffer
    RJStringBuffer buffer;
    ptr_RJStringWriter writer = std::make_shared<RJStringWriter>(buffer);

    writer->StartObject();
    writer->String("metadata");

    writer->StartObject();
    writer->String("id");
    writer->Int64(vertex_accessor.id());
    writer->EndObject();

    writer->String("data");
    writer->StartObject();
    // RapidJsonStringWriter dataBuffer(writer);
    // auto properties = vertex_accessor.properties();
    // properties.accept(dataBuffer);
    writer->EndObject();

    writer->EndObject();

    return std::move(buffer.GetString());
}
