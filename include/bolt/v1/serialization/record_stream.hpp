#pragma once

#include "bolt/v1/serialization/bolt_serializer.hpp"
#include "bolt/v1/transport/chunked_buffer.hpp"
#include "bolt/v1/transport/chunked_encoder.hpp"
#include "bolt/v1/transport/socket_stream.hpp"

#include "logging/default.hpp"

namespace bolt
{

// compiled queries have to use this class in order to return results
// query code should not know about bolt protocol

template <class Socket>
class RecordStream
{
public:
    RecordStream(Socket &socket) : socket(socket)
    {
        logger = logging::log->logger("Record Stream");
    }

    // TODO: create apstract methods that are not bolt specific ---------------
    void write_success()
    {
        logger.trace("write_success");
        bolt_encoder.message_success();
    }

    void write_success_empty()
    {
        logger.trace("write_success_empty");
        bolt_encoder.message_success_empty();
    }

    void write_ignored()
    {
        logger.trace("write_ignored");
        bolt_encoder.message_ignored();
    }

    void write_fields(const std::vector<std::string> &fields)
    {
        // TODO: that should be one level below?
        bolt_encoder.message_success();

        bolt_encoder.write_map_header(1);
        bolt_encoder.write_string("fields");
        write_list_header(fields.size());

        for (auto &name : fields) {
            bolt_encoder.write_string(name);
        }

        flush();
    }

    void write_list_header(size_t size)
    {
        bolt_encoder.write_list_header(size);
    }

    void write_record()
    {
        bolt_encoder.message_record();
    }
    // -- BOLT SPECIFIC METHODS -----------------------------------------------

    void write(const Vertex::Accessor &vertex) { serializer.write(vertex); }
    void write(const Edge::Accessor &edge) { serializer.write(edge); }

    void write(const Property &prop) { serializer.write(prop); }
    void write(const Bool& prop) { serializer.write(prop); }
    void write(const Float& prop) { serializer.write(prop); }
    void write(const Int32& prop) { serializer.write(prop); }
    void write(const Int64& prop) { serializer.write(prop); }
    void write(const Double& prop) { serializer.write(prop); }
    void write(const String& prop) { serializer.write(prop); }

    void flush()
    {
        chunked_encoder.flush();
        chunked_buffer.flush();
    }

    void _write_test()
    {
        logger.trace("write_test");

        write_fields({{"name"}});

        write_record();
        write_list_header(1);
        write(String("max"));

        write_record();
        write_list_header(1);
        write(String("paul"));

        write_success_empty();
    }

protected:
    Logger logger;

private:
    using buffer_t = ChunkedBuffer<SocketStream>;
    using chunked_encoder_t = ChunkedEncoder<buffer_t>;
    using bolt_encoder_t = BoltEncoder<chunked_encoder_t>;
    using bolt_serializer_t = BoltSerializer<bolt_encoder_t>;

    SocketStream socket;
    buffer_t chunked_buffer{socket};
    chunked_encoder_t chunked_encoder{chunked_buffer};
    bolt_encoder_t bolt_encoder{chunked_encoder};
    bolt_serializer_t serializer{bolt_encoder};
};
}
