#include <iostream>

#include "speedy/speedy.hpp"
#include "resource.hpp"

class Animal : public api::Resource<Animal, api::GET, api::POST>
{
public:
    Animal(speedy::Speedy& app) : Resource(app, "/animal") {}

    void get(http::Request& req, http::Response& res)
    {
        return res.send("Ok, here is a Dog");
    }

    void post(http::Request& req, http::Response& res)
    {
        return res.send("Oh, you gave me an animal?");
    }
};

int main(void)
{
    uv::UvLoop loop;
    speedy::Speedy app(loop);

    auto animal = Animal(app);

    http::Ipv4 ip("0.0.0.0", 3400);
    app.listen(ip);

    loop.run(uv::UvLoop::Mode::Default);

    return 0;
}
