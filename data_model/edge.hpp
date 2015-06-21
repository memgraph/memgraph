#ifndef MEMGRAPH_DATA_MODEL_EDGE_HPP
#define MEMGRAPH_DATA_MODEL_EDGE_HPP

template <class T>
struct Node;

template <class T>
struct Edge
{

    Node<T>* from;
    Node<T>* to;   
    T* data;
};

#endif
