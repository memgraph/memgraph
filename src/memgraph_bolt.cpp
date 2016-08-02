#include "utils/terminate_handler.hpp"

int main(int argc, char *argv[])
{
    if (argc < 2) {
        std::cout << "Port not defined" << std::endl;
        std::exit(0);
    }

    auto port = std::stoi(argv[1]);

    std::cout << "Port is: " << port << std::endl;

    std::set_terminate(&terminate_handler);

    return 0;
}
