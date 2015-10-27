#include <iostream>
#include <fstream>
#include <sstream> 
#include <vector>
#include <iterator>
#include <cstdlib>
#include "example/db.hpp"
#include "dynamic_lib.hpp"

using std::cout;
using std::endl;

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

// dependent on specific dynamic code
// "configuration" of DynamicLib
// DynamicLib<MemgraphDynamicLib>
class MemgraphDynamicLib
{
public:
    const static std::string produce_name;
    const static std::string destruct_name;
    typedef produce_t produce;
    typedef destruct_t destruct;
};
const std::string MemgraphDynamicLib::produce_name = "produce";
const std::string MemgraphDynamicLib::destruct_name = "destruct";

int main() 
{
    // -- compile example
    
    // string tmp_file_path = "tmp/tmp.cpp";
    // string tmp_so_path = "tmp/tmp.so";
    // string for_compile = "#include <iostream>\nint main() { std::cout << \"test\" << std::endl; return 0; }";

    // write(tmp_file_path, for_compile);
    // string test_command = prints("clang++", tmp_file_path, "-o", "test.out");
    // system(test_command.c_str());
    
    // -- end compile example

    // -- load example
    using db_lib = DynamicLib<MemgraphDynamicLib>;

    db_lib mysql_db("./tmp/mysql.so");
    mysql_db.load();
    auto mysql = mysql_db.produce_method();
    if (mysql) {
        mysql->name();
    }
    mysql_db.destruct_method(mysql);

    db_lib memsql_db("./tmp/memsql.so");
    memsql_db.load();
    auto memsql = memsql_db.produce_method();
    if (memsql) {
        memsql->name();
    }
    memsql_db.destruct_method(memsql);

    return 0;
}
