#include <iostream>
#include <fstream>
#include <sstream> 
#include <vector>
#include <iterator>
#include <cstdlib>
#include <dlfcn.h>
#include "example/db.hpp"

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

db* database(const std::string& lib_path, const std::string& factory_method)
{
    // load lib
    void* db_lib = dlopen(lib_path.c_str(), RTLD_LAZY);
    if (!db_lib) {
        cerr << "Cannot load library: " << dlerror() << '\n';
        return nullptr;
    }
    dlerror();

    // load produce method
    produce_t produce_db = (produce_t) dlsym(db_lib, factory_method.c_str());
    const char* dlsym_error = dlerror();
    if (dlsym_error) {
        cerr << "Cannot load symbol create: " << dlsym_error << '\n';
        return nullptr;
    }

    // load destroy method
    // destruct_t destruct_db = (destruct_t) dlsym(db_lib, "destruct");
    // dlsym_error = dlerror();
    // if (dlsym_error) {
    //     cerr << "Cannot load symbol destroy: " << dlsym_error << '\n';
    //     return nullptr;
    // }

    db *instance = produce_db();

    return instance;
}

int main() 
{
    // string tmp_file_path = "tmp/tmp.cpp";
    // string tmp_so_path = "tmp/tmp.so";
    // string for_compile = "#include <iostream>\nint main() { std::cout << \"test\" << std::endl; return 0; }";

    // write(tmp_file_path, for_compile);
    // string test_command = prints("clang++", tmp_file_path, "-o", "test.out");
    // system(test_command.c_str());

    db *mysql = database("./tmp/mysql.so", "produce");
    if (mysql) {
        mysql->name();
    }

    return 0;
}
