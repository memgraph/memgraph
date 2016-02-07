#include <iostream>
#include <fstream>
#include <vector>
#include <iterator>
#include <cstdlib>
#include "example/db.hpp"
#include "dynamic_lib.hpp"
#include "utils/string/file.hpp"

using std::cout;
using std::endl;

// dependent on specific dynamic code
// "configuration" of DynamicLib
// DynamicLib<MemgraphDynamicLib>
class MemgraphDynamicLib
{
public:
    const static std::string produce_name;
    const static std::string destruct_name;
    using produce = produce_t;
    using destruct = destruct_t;
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
