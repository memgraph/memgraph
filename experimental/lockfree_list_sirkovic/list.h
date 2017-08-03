#include <iostream>
#include <algorithm>
#include <cstdio>
#include <vector>
#include <string>
#include <cstdlib>
#include "node.h"

using namespace std;

template<typename T>
struct list_t {
  node_t<T> *start_ptr;
  node_t<T> *end_ptr;
  atomic<int> length;

  list_t<T> (const T& a,const T& b) {
    end_ptr = allocate_node(b, NULL);
    start_ptr = allocate_node(a, end_ptr);
    length.store(0);
  }

  node_t<T> *allocate_node(const T& data, node_t<T>* next) {
    return new node_t<T>(data,next);
  }

  node_t<T> *find(const T& val, node_t<T> **left){
    //printf("find node %d\n",val);
    node_t<T> *left_next, *right, *left_next_copy;
    left_next = right = NULL;
    while(1) {
      node_t<T> *it = start_ptr;
      node_t<T> *it_next = start_ptr->next.load();
      //while (get_flag(it_next) || (it->data.load() < val)) {
      while (get_flag(it_next) || (it->data < val)) {
        //printf("%d\n",it->data);
        if (!get_flag(it_next)) {
          (*left) = it;
          left_next = it_next;
        }
        it = get_unflagged(it_next);
        if (it == end_ptr) break;
        //it_next = it->next.load(memory_order_relaxed);
        it_next = it->next.load();
      }
      right = it;left_next_copy = left_next;

      if (left_next == right){
        //if (!get_flag(right->next.load(memory_order_relaxed)))
        if (right == end_ptr || !get_flag(right->next.load()))
          return right;
      }
      else {
        if ((*left)->next.compare_exchange_strong(left_next_copy,right) == true) {
          int previous = left_next->ref_count.fetch_add(-1);
          previous = right->ref_count.fetch_add(1);
          //if (!get_flag(right->next.load(memory_order_relaxed))) return right;
          if (!get_flag(right->next.load())) return right;
        }
      }
    }
  }

  int contains(const T& val) {
    //printf("search node %d\n",val);
    //node_t<T> *it = get_unflagged(start_ptr->next.load(memory_order_relaxed));
    node_t<T> *it = get_unflagged(start_ptr->next.load());
    while(it != end_ptr) {
      //if (!get_flag(it->next) && it->data.load() >= val){
      if (!get_flag(it->next) && it->data >= val){
        //if (it->data.load() == val) return 1;
        if (it->data == val) return 1;
        else return 0;
      }
      //it = get_unflagged(it->next.load(memory_order_relaxed));
      it = get_unflagged(it->next.load());
    }
    return 0;
  }

  int size() {
    return length.load();
  }

  int add(const T& val) {
    //printf("add node %d\n",val);
    node_t<T> *right, *left;
    right = left = NULL;
    node_t<T> *new_elem = allocate_node(val, NULL);
    while(1) {
      right = find(val, &left);
      //if (right != end_ptr && right->data.load() == val){
      if (right != end_ptr && right->data == val){
        return 0;
      }
      new_elem->next.store(right);
      if (left->next.compare_exchange_strong(right,new_elem) == true) {
        length.fetch_add(1);
        return 1;
      }
      else {
      }
    }
  }

  node_t<T>* remove(const T& val) {
    //printf("remove node %d\n",val);
    node_t<T>* right, *left, *right_next, *tmp;
    node_t<T>* left_next, *right_copy;
    right = left = right_next = tmp = NULL;
    while(1) {
      right = find(val, &left);
      left_next = left->next.load();
      right_copy = right;
      //if (right == end_ptr || right->data.load() != val){
      if (right == end_ptr || right->data != val){
        return NULL;
      }
      //right_next = right->next.load(memory_order_relaxed);
      right_next = right->next.load();
      if (!get_flag(right_next)){
        node_t<T>* right_next_marked = get_flagged(right_next);
        if ((right->next).compare_exchange_strong(right_next,right_next_marked)==true) {
          if((left->next).compare_exchange_strong(right_copy,right_next) == false) {
            tmp = find(val,&tmp);
          } else {
            int previous = right->ref_count.fetch_add(-1);

            previous = right_next->ref_count.fetch_add(1);
          }
          length.fetch_add(-1);
          return right;
        }
      }
    }
  }

  int get_flag(node_t<T>* ptr) {
    return is_marked(reinterpret_cast<long long>(ptr));
  }

  void mark_flag(node_t<T>* &ptr){
    ptr = get_flagged(ptr);
  }

  void unmark_flag(node_t<T>* &ptr){
    ptr = get_unflagged(ptr);
  }

  inline static node_t<T>* get_flagged(node_t<T>* ptr){
    return reinterpret_cast<node_t<T>*>(get_marked(reinterpret_cast<long long>(ptr)));
  }

  inline static node_t<T>* get_unflagged(node_t<T>* ptr){
    return reinterpret_cast<node_t<T>*>(get_unmarked(reinterpret_cast<long long>(ptr)));
  }

  struct iterator{
    node_t<T>* ptr;
    iterator(node_t<T>* ptr_) : ptr(ptr_) {
      ptr->ref_count.fetch_add(1);
    }
    ~iterator() {
      if(ptr != NULL) ptr->ref_count.fetch_add(-1);
    }
    bool operator==(const iterator& other) {
      return ptr == other.ptr;
    }
    bool operator!=(const iterator& other) {
      return ptr != other.ptr;
    }
    iterator& operator++() {

      node_t<T>* it_next = ptr->next.load(), *it = ptr, *it_next_unflagged = list_t<T>::get_unflagged(it_next);
      while(it_next_unflagged != NULL && it_next != it_next_unflagged) {
        it = it_next_unflagged;
        it_next = it->next.load();
        it_next_unflagged = list_t<T>::get_unflagged(it_next);
      }
      if(it_next_unflagged == NULL) {
        it->ref_count.fetch_add(1);
        ptr->ref_count.fetch_add(-1);
        ptr = it;
      } else {
        it_next->ref_count.fetch_add(1);
        ptr->ref_count.fetch_add(-1);
        ptr = it_next;
      }
      return *this;
    }
    T& operator*() {
      return ptr->data;
    }
  };

  iterator begin(){
    while(1) {
      node_t<T>* it = start_ptr->next.load();
      node_t<T>* it_next = it->next.load();
      while(it!=end_ptr && get_flag(it->next.load())) {
        it = it_next;
        it_next = it_next->next.load();
      }
      if(it == end_ptr) return end();
      return iterator(it_next);
    }
  }
  iterator end(){
    return iterator(end_ptr);
  }

};
