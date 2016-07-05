#include <iostream>
#include <utility>

#include "storage/indexes/index.hpp"

// boilerplate
using std::cout;
using std::endl;

// types
using StringUniqueKeyAsc = UniqueKeyAsc<std::shared_ptr<std::string>>;
using index_t = Index<StringUniqueKeyAsc, std::string>;

int main(void)
{
    // index creation
    auto index = std::make_shared<index_t>();

    // prepare values
    StringUniqueKeyAsc key(std::make_shared<std::string>("test_key"));
    auto value_ptr = std::make_shared<std::string>("test_value");

    // insert into and unpack pair
    index_t::skiplist_t::Iterator find_iterator;
    bool insertion_succeeded;
    std::tie(find_iterator, insertion_succeeded) = 
        index->insert(key, value_ptr.get());
    assert(insertion_succeeded == true);
    
    // get inserted value
    auto inserted_value = *index->find(key);
    assert(*inserted_value.second == *value_ptr);

    return 0;
}
