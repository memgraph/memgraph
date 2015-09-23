#include <iostream>

#include "speedy.hpp"

const char *test_url_1 = "/test1";
const char *test_url_2 = "/test2";
const char *test_url_3 = "/test3";
const char *test_response = "test";

void test_get(const http::request_cb_t &&callback, speedy::Speedy &app) {
    app.get(test_url_3, callback);
}

void test_post(const http::request_cb_t &&callback, speedy::Speedy &app) {
    app.post(test_url_3, callback);
}

void test_put(const http::request_cb_t &&callback, speedy::Speedy &app) {
    app.put(test_url_3, callback);
}

void test_delete(const http::request_cb_t &&callback, speedy::Speedy &app) {
    app.del(test_url_3, callback);
}

auto test_callback = [](http::Request& req, http::Response& res) {
    res.send(test_response);
};

int main(void)
{
    // speedy init
    uv::UvLoop loop;
    http::Ipv4 ip("0.0.0.0", 3400);
    speedy::Speedy app(loop, ip);

    // GET methods
    app.get(test_url_1, test_callback);
    app.get(test_url_2, [](http::Request& req, http::Response& res) {
        res.send(test_response);
    });
    test_get(test_callback, app);
    // POST examples
    app.post(test_url_1, test_callback);
    app.post(test_url_2, [](http::Request& req, http::Response& res) {
        res.send(test_response);
    });
    test_post(test_callback, app);
    // PUT examples
    app.put(test_url_1, test_callback);
    app.put(test_url_2, [](http::Request& req, http::Response& res) {
        res.send(test_response);
    });
    test_put(test_callback, app);
    // DELETE examples
    app.del(test_url_1, test_callback);
    app.del(test_url_2, [](http::Request& req, http::Response& res) {
        res.send(test_response);
    });
    test_delete(test_callback, app);

    // app run
    app.listen();
    loop.run(uv::UvLoop::Mode::Default);

    return 0;
}
