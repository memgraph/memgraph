#include <iostream>

#include "speedy.hpp"

// TODO: still doesn't work
// debug the whole thing
void test(http::request_cb_t callback, speedy::Speedy &app) {
    app.get("/test", callback); 
}

auto foo = [](http::Request& req, http::Response& res) {
    res.send("foo");
};

int main(void)
{
    uv::UvLoop loop;
    http::Ipv4 ip("0.0.0.0", 3400);

    speedy::Speedy app(loop, ip);
    app.get("/foo", foo);
    app.get("/bar", [](http::Request& req, http::Response& res) {
        res.send("bar");
    });
    auto cb = [](http::Request& req, http::Response& res) {
        res.send("test");
    };
    test(http::request_cb_t(cb), app);

    app.listen();

    loop.run(uv::UvLoop::Mode::Default);

    return 0;
}
