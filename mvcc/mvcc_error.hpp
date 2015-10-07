#ifndef MEMGRAPH_MVCC_MVCC_ERROR_HPP
#define MEMGRAPH_MVCC_MVCC_ERROR_HPP

#include <stdexcept>

namespace mvcc
{

class MvccError : public std::runtime_error
{
public:
    using runtime_error::runtime_error;
};

}

#endif
