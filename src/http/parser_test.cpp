#include <iostream>

#include "io/network/tcp_server.hpp"
#include "http/worker.hpp"

int main(void)
{
    const char* req = "GET /foo/bar/7/comments HTTP/1.1\r\n"
                       "Content-Type : application/javascript\r\n"
                       "Content-Length: 3872\r\n"
                       "Connection:keep-alive\r\n"
                       "Accept-Ranges: bytes\r\n"
                       "Last-modified: Sun, 8 october 2015 GMT\r\n"
                       "\r\n"
                       "{alo:'bre', bre: 'alo sta ti je'}\r\n";

    io::TcpServer<http::Worker> server;

    server.bind("0.0.0.0", "7474").listen(8, 128, []() {
        std::cout << "response!" << std::endl;
    });

    return 0;
};
