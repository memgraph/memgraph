#pragma once

#include "utils/crtp.hpp"
#include <atomic>

template <class T>
static T load(std::atomic<T> &atomic)
{
    return atomic.load(std::memory_order_acquire);
}

template <class T>
static void store(std::atomic<T> &atomic, T desired)
{ // Maybe could be relaxed
    atomic.store(desired, std::memory_order_release);
}

template <class T>
static bool cas(std::atomic<T> &atomic, T expected, T desired)
{ // Could be relaxed must be atleast Release.
    return atomic.compare_exchange_strong(expected, desired,
                                          std::memory_order_seq_cst);
}

template <class T>
static T *swap(std::atomic<T *> &atomic, T *desired)
{ // Could be relaxed
    return atomic.exchange(desired, std::memory_order_seq_cst);
}

template <class T>
class List
{

private:
    class Node
    {
    public:
        Node(const T &data) : data(data) {}
        Node(T &&data) : data(std::forward(data)) {}

        T data;
        std::atomic<Node *> next{nullptr};
        std::atomic<Node *> next_rem{nullptr};
        std::atomic<bool> removed{false};
    };

    template <class It>
    class IteratorBase : public Crtp<It>
    {
        friend class List;

    protected:
        IteratorBase() : list(nullptr), curr(nullptr) {}

        IteratorBase(List *list) : list(list)
        {
            list->count++;
            reset();
        }

    public:
        IteratorBase(const IteratorBase &) = delete;

        IteratorBase(IteratorBase &&other)
            : list(other.list), curr(other.curr), prev(other.prev)
        {
            other.list = nullptr;
            other.curr = nullptr;
        }

        ~IteratorBase()
        {
            if (list == nullptr) {
                return;
            }

            auto head_rem = load(list->removed);
            // Fetch could be relaxed
            // There exist possibility that no one will delete garbage at this
            // time.
            if (list->count.fetch_sub(1) == 1 && head_rem != nullptr &&
                cas<Node *>(
                    list->removed, head_rem,
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
            return curr->data;
        }
        T *operator->()
        {
            assert(valid());
            return curr->data;
        }
        operator T *()
        {
            assert(valid());
            return curr->data;
        }

        bool valid() { return curr != nullptr; }

        // Iterating is wait free.
        It &operator++()
        {
            assert(valid());
            do {
                prev = curr;
                curr = load(curr->next);
            } while (valid() && is_removed());
            return this->derived();
        }
        It &operator++(int) { return operator++(); }

        bool is_removed()
        {
            assert(valid());
            return load(curr->removed);
        }

        // Returns IteratorBase to begining
        void reset()
        {
            prev = nullptr;
            curr = load(list->head);
            while (valid() && is_removed()) {
                operator++();
            }
        }

        // Adds to the begining of list
        // It is lock free but it isn't wait free.
        void push(T &&data)
        {
            auto node = new Node(data);
            Node *next = nullptr;
            do {
                next = load(list->head);
                store(node->next, next);
            } while (!cas(list->head, next, node));
        }

        // True only if this call removed the element. Only reason for fail is
        // if
        // the element is already removed.
        // Remove has deadlock if another thread dies between marking node for
        // removal
        // and the disconnection.
        // This can be improved with combinig the removed flag with prev.next or
        // curr.next
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

        friend bool operator==(const It &a, const It &b)
        {
            return a.curr == b.curr;
        }

        friend bool operator!=(const It &a, const It &b) { return !(a == b); }

    private:
        void find_and_disconnect()
        {
            auto it = It(list);
            auto next = load(curr->next);
            while (it.valid()) {
                if (it.curr == curr) {
                    if (it.disconnect()) {
                        return;
                    }
                    it.reset();
                } else if (it.curr == next) { // Comparison with next is
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
    class ConstIterator : public IteratorBase<ConstIterator>
    {
        friend class List;

    public:
        using IteratorBase<ConstIterator>::IteratorBase;

        const T &operator*()
        {
            return IteratorBase<ConstIterator>::operator*();
        }

        const T *operator->()
        {
            return IteratorBase<ConstIterator>::operator->();
        }

        operator const T &()
        {
            return IteratorBase<ConstIterator>::operator T &();
        }
    };

    class Iterator : public IteratorBase<Iterator>
    {
        friend class List;

    public:
        using IteratorBase<Iterator>::IteratorBase;
    };

public:
    List() = default;

    List(List &) = delete;
    List(List &&) = delete;

    void operator=(List &) = delete;

    Iterator begin() { return Iterator(this); }

    ConstIterator begin() const { return ConstIterator(this); }

    ConstIterator cbegin() const { return ConstIterator(this); }

    Iterator end() { return Iterator(); }

    ConstIterator end() const { return ConstIterator(); }

    ConstIterator cend() const { return ConstIterator(); }

private:
    std::atomic<size_t> count{0};
    std::atomic<Node *> head{nullptr};
    std::atomic<Node *> removed{nullptr};
};
