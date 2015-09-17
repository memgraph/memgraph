#include <iostream>

#include "application/application.hpp"

int main(void)
{
    http::Ipv4 ip("0.0.0.0", 3400);

    application::Router router(ip);
    router.listen();

    return 0;
}
