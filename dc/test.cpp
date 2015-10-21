#include <iostream>
#include <fstream>
#include <sstream> 
#include <vector>
#include <iterator>
#include <cstdlib>

// TODO: clang++ file.cpp -o file.so -shared -fPIC

using namespace std;

void write(const std::string& path, const std::string& content)
{
    ofstream stream;
    stream.open (path.c_str());
    stream << content;
    stream.close();
}

std::string join(const std::vector<std::string>& strings, const char *separator)
{
    std::ostringstream oss;
    std::copy(strings.begin(), strings.end(),
              std::ostream_iterator<std::string>(oss, separator));
    return oss.str();
}

template<typename... Args>
std::string prints(const Args&... args)
{
    std::vector<std::string> strings = {args...};
    return join(strings, " ");
}

int main() 
{
    string tmp_file_path = "tmp/tmp.cpp";
    string tmp_so_path = "tmp/tmp.so";
    string for_compile = "#include <iostream>\nint main() { std::cout << \"test\" << std::endl; return 0; }";

    write(tmp_file_path, for_compile);
    string test_command = prints("clang++", tmp_file_path, "-o", "test.out");
    system(test_command.c_str());

    return 0;
}
