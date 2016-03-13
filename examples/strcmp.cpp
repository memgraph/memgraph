#include <iostream>
#include <fstream>
#include <cstring>
std::string load_file(const std::string& fname)
{
    std::ifstream in(fname);
    return std::string((std::istreambuf_iterator<char>(in)),
                        std::istreambuf_iterator<char>());
}


int main(int argc, const char* argv[])
{
    if(argc < 3)
        return -1;

    auto a = load_file(argv[1]);
    auto b = load_file(argv[2]);

    bool result = true;

    for(size_t i = 0; i < a.size(); ++i)
        result &= strcmp(a.c_str() + i, b.c_str() + i) == 0;

    std::cout << result << std::endl;
    return 0;
}
