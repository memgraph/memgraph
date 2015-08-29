#ifndef MEMGRAPH_SERVER_HTTP_RESPONSE_INL
#define MEMGRAPH_SERVER_HTTP_RESPONSE_INL

#include "response.hpp"
#include "httpconnection.hpp"

namespace http
{

Response::Response(HttpConnection& connection)
    : connection(connection), buffer(65536), code(Status::Ok) {}

void Response::send(Status code, const std::string& body)
{
    this->status(code).send(body);
}

void Response::send(const std::string& body)
{
    uv_write_t* write_req =
        static_cast<uv_write_t*>(malloc(sizeof(uv_write_t)));

    write_req->data = &connection;

    // set the appropriate content length
    headers["Content-Length"] = std::to_string(body.size());

    // set the appropriate connection type
    headers["Connection"] = connection.keep_alive ? "Keep-Alive" : "Close";

    buffer << "HTTP/1.1 " << to_string[code] <<  "\r\n";

    for(auto it = headers.begin(); it != headers.end(); ++it)
        buffer << it->first << ":" << it->second << "\r\n";

    buffer << "\r\n" << body;

    uv_write(write_req, connection.client, buffer, 1,
            [](uv_write_t* write_req, int) {

        HttpConnection& conn =
            *reinterpret_cast<HttpConnection*>(write_req->data);

        if(!conn.keep_alive)
            conn.close();

        conn.response.code = Status::Ok;
        conn.response.buffer.clear();
        conn.response.headers.clear();
        std::free(write_req);      
    });
}

Response& Response::status(Status code)
{
    this->code = code;
    return *this;
}

}

#endif
