#include <iostream>

#include "data_structures/concurrent/skiplist.hpp"
#include "utils/time/timer.hpp"

int main(void)
{
    SkipList<int> skiplist;

    // insert a bunch of elements
    {
        int iters_no = 1000000;
        auto skiplist_accessor = skiplist.access();
        Stopwatch sw;
   
        sw.Start(); 
        for (int i = 0; i < iters_no; ++i)
        {
            skiplist_accessor.insert(std::move(i));    
        }

        std::cout << "Skiplist contains: "
                  << skiplist_accessor.size()
                  << " elements"
                  << std::endl;
    }

    return 0;
}
