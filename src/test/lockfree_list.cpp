#include <iostream>

#include <memory>
#include "memory/hp.hpp"
#include "data_structures/list/lockfree_list.hpp"
#include "mvcc/atom.hpp"

using namespace std;

template <class T>
class ListNode {
public:
    explicit ListNode(T value) : value(value) {}

    std::atomic<T> value;
    std::atomic<ListNode<T>*> prev;
    std::atomic<ListNode<T>*> next;
};

using lf_list = lockfree::List<ListNode<int>>;
using sptr_listnode = std::shared_ptr<lf_list>;

void test(sptr_listnode& list)
{    
    auto& hp = memory::HP::get();
    
    auto head = list->begin();
    cout << hp;
    cout << "Element: " << (*head).value << endl;

    ++head;
    cout << hp;
    cout << "Element: " << (*head).value << endl;
}

void print_list(sptr_listnode& list)
{
    cout << "Lockfree list: ";
    auto head = list->begin();
    while (head->next != nullptr) {
        cout << (*head).value << " ";
        ++head;
    }
    cout << (*head).value << " ";
    cout << endl;
}

int main()
{
    auto& hp = memory::HP::get();
    sptr_listnode list(new lf_list());

    list->push_front(new ListNode<int>(1));
    list->push_front(new ListNode<int>(2));
    list->push_front(new ListNode<int>(3));
   
    cout << "Initial list" << endl; 
    print_list(list);

    test(list);
    cout << "After test method" << endl;

    // remove element from the list
    auto rw_head = list->rw_begin();
    ++rw_head;
    ++rw_head;
    list->remove(rw_head);
    cout << "After remove method" << endl;

    cout << "Final list" << endl;
    print_list(list);

    cout << "Final HP list (before main exit, some hp references may still exist)" << endl;
    cout << hp;

    return 0;
}
