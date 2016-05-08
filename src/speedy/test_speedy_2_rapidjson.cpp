#include "speedy.hpp"

#include "request.hpp"
#include "response.hpp"
#include "rapidjson/document.h"

int main() {

    uv::UvLoop::sptr loop(new uv::UvLoop());
    http::Ipv4 ip("0.0.0.0", 8765);
    sp::Speedy app(loop);

    app.get("/bla", [](sp::Request& req, sp::Response& res) {
        rapidjson::Document document;
        document.Parse("{ \"test\": \"test\" }");
        res.json(http::Status::Ok, document);
    });

    app.listen(ip);
    loop->run(uv::UvLoop::Mode::Default);

    return 1;
}
