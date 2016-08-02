#pragma once

#include <atomic>

template <class T>
static T *load(std::atomic<T *> &atomic)
{
    return atomic.load(std::memory_order_acquire())
}

template <class T>
static void store(std::atomic<T *> &atomic, T *desired)
{ // Maybe could be relaxed
    atomic.store(desired, std::memory_order_release());
}

template <class T>
static bool cas(std::atomic<T *> &atomic, T *expected, T *desired)
{ // Could be relaxed must be atleast Release.
    return atomic.compare_exchange_strong(expected,desired,std::memory_order_seq_cst()));
}

template <class T>
static T *swap(std::atomic<T *> &atomic, T *desired)
{ // Could be relaxed
    return atomic.exchange(desired,std::memory_order_seq_cst()));
}

template <class T>
class List
{
    class Node
    {
        friend class Iterator;

    private:
        Node(const T &data) : data(data) {}
        Node(T &&data) : data(std::forward(data)) {}

        T data;
        std::atomic<Node *> next{nullptr};
        std::atomic<Node *> next_rem{nullptr};
        std::atomic<bool> removed{false};
    };

    class Iterator
    {
        friend class List;

        Iterator() : list(nullptr), curr(nullptr) {}

        Iterator(List *list) : list(list)
        {
            list->count++;
            reset();
        }

    public:
        Iterator(const Accessor &) = delete;

        Iterator(Iterator &&other)
            : list(other.list), curr(other.curr), prev(other.prev)
        {
            other.list = nullptr;
            other.curr = nullptr;
        }

        ~Iterator()
        {
            if (list == nullptr) {
                return;
            }

            auto head_rem = load(list->head_rem);
            // Fetch could be relaxed
            // There exist possibility that no one will delete garbage at this
            // time.
            if (list.count.fetch_sub(1) == 1 && head_rem != nullptr &&
                cas(list->head_rem, head_rem,
                    nullptr)) { // I am the last one and there is garbage to be
                                // removed.
                auto now = head_rem;
                do {
                    auto next = load(now->next_rem);
                    delete now;
                    now = next;
                } while (now != nullptr);
            }
        }

        T &operator*()
        {
            assert(valid());
            return *curr;
        }
        T *operator->()
        {
            assert(valid());
            return curr;
        }
        operator T *()
        {
            assert(valid());
            return curr;
        }

        bool valid() { return curr != nullptr; }

        Iterator &operator++()
        {
            assert(valid());
            do {
                prev = curr;
                curr = load(curr->next);
            } while (valid() && is_removed());
            return this;
        }

        bool is_removed()
        {
            assert(valid());
            return load(curr->removed);
        }

        // Returns iterator to begining
        void reset()
        {
            prev = nullptr;
            curr = load(list->head);
            while (valid() && is_removed()) {
                this ++;
            }
        }

        // Adds to the begining of list
        void push(T &&data)
        {
            auto node = new Node(data);
            auto next = nullptr;
            do {
                next = load(list->head);
                store(next.next, next);
            } while (!cas(list->head, next, node));
        }

        // True only if this call removed the element.
        bool remove()
        {
            assert(valid());
            if (cas(curr->removed, false, true)) {
                if (!disconnect()) {
                    find_and_disconnect();
                }
                store(curr->next_rem, swap(list->removed, curr));
                return true;
            }
            return false;
        }

        friend bool operator==(const Iterator &a, const Iterator &b)
        {
            return a->curr == b->curr;
        }

        friend bool operator!=(const Iterator &a, const Iterator &b)
        {
            return !(a == b);
        }

    private:
        void find_and_disconnect()
        {
            auto it = Iterator(list);
            auto next = load(curr->next);
            while (it.valid()) {
                if (it.succ == succ) {
                    if (it.disconnect()) {
                        return;
                    }
                    it.reset();
                } else if (it.succ == next) { // Comparison with next is
                                              // optimization for early return.
                    return;
                } else {
                    it++;
                }
            }
        }

        bool disconnect()
        {
            auto next = load(curr->next);
            if (prev != nullptr) {
                store(prev->next, next);
                if (load(prev->removed)) {
                    return false;
                }
            } else if (!cas(list->head, curr, next)) {
                return false;
            }
            return true;
        }

        List *list;
        Node *prev{nullptr};
        Node *curr;
    };

public:
    List() = default;

    List(List &) = delete;
    List(List &&) = delete;

    void operator=(List &) = delete;

    Iterator begin() { return Iterator(this); }

    Iterator end() { return Iterator(); }

private:
    std::atomic<size_t> count{0};
    std::atomic<Node *> head{nullptr};
    std::atomic<Node *> removed{nullptr};
}
