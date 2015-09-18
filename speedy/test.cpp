#include <iostream>

#include "speedy.hpp"

int main(void)
{
    http::Ipv4 ip("0.0.0.0", 3400);
    speedy::Speedy app(ip);
    app.listen();

    return 0;
}
