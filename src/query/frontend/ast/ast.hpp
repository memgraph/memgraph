#pragma once

#include <memory>
#include <vector>

#include "database/graph_db.hpp"

namespace query {

template <typename T>
using sptr = std::shared_ptr<T>;

template <typename T>
using uptr = std::unique_ptr<T>;

class Tree {
public:
    Tree(const int uid) : uid_(uid) {}
    int uid() const { return uid_; }
private:
    const int uid_;
};

class Expr : public Tree {
};

class Ident : public Tree {
public:
    std::string identifier_;
};

class Part {
};

class NodePart : public Part {
public:
    Ident identifier_;
    // TODO: Mislav call GraphDb::label(label_name) to populate labels_!
    std::vector<GraphDb::Label> labels_;
    // TODO: properties
};

}
