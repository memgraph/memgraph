#include <iostream>
#include <queue>
#include <string>
#include <vector>

#include "query_engine/i_code_cpu.hpp"
#include "storage/model/properties/all.hpp"
#include "utils/memory/stack_allocator.hpp"

using std::cout;
using std::endl;

// Dressipi astar query of 4 clicks.

// BARRIER!
namespace barrier
{

// using STREAM = std::ostream;
using STREAM = RecordStream<::io::Socket>;

constexpr size_t max_depth = 3;
constexpr size_t limit = 10;

class Node
{
public:
    Node *parent = {nullptr};
    VertexPropertyType<Double> tkey;
    double cost;
    int depth = {0};
    VertexAccessor vacc;

    Node(VertexAccessor vacc, double cost,
         VertexPropertyType<Double> const &tkey)
        : cost(cost), vacc(vacc), tkey(tkey)
    {
    }
    Node(VertexAccessor vacc, double cost, Node *parent,
         VertexPropertyType<Double> const &tkey)
        : cost(cost), vacc(vacc), parent(parent), depth(parent->depth + 1),
          tkey(tkey)
    {
    }

    double sum_vertex_score()
    {
        auto now = this;
        double sum = 0;
        do {
            sum += (now->vacc.at(tkey).get())->value();
            now = now->parent;
        } while (now != nullptr);
        return sum;
    }
};

bool vertex_filter_contained(DbAccessor &t, VertexAccessor &v, Node *before)
{
    if (v.fill()) {
        bool found;
        do {
            found = false;
            before = before->parent;
            if (before == nullptr) {
                return true;
            }
        } while (v.in_contains(before->vacc));
    }
    return false;
}

void astar(DbAccessor &t, code_args_t &args, STREAM &stream)
{
    StackAllocator stack;

    VertexPropertyType<Double> tkey = t.vertex_property_key<Double>("score");

    auto cmp = [](Node *left, Node *right) { return left->cost > right->cost; };
    std::priority_queue<Node *, std::vector<Node *>, decltype(cmp)> queue(cmp);
    std::vector<Node *> all_nodes;

    auto start_vr = t.vertex_find(Id(args[0].as<Int64>().value()));
    if (!start_vr.is_present()) {
        stream.write_failure({{}});
        return;
    }

    start_vr.get().fill();
    Node *start = stack.make<Node>(start_vr.take(), 0, tkey);
    queue.push(start);
    all_nodes.push_back(start);

    int count = 0;
    do {
        auto now = queue.top();
        queue.pop();

        if (max_depth <= now->depth) {
            // best.push_back(now);
            count++;
            if (count >= limit) {
                break;
            }
            continue;
        }

        iter::for_all(now->vacc.out(), [&](auto edge) {
            VertexAccessor va = edge.to();
            if (vertex_filter_contained(t, va, now)) {
                auto cost = 1 - va.at(tkey).get()->value();
                Node *n = stack.make<Node>(va, now->cost + cost, now, tkey);
                queue.push(n);
                all_nodes.push_back(n);
            }
        });
    } while (!queue.empty());

    stream.write_field("n");
    stream.write_record();
    stream.write_list_header(0);
    stream.chunk();
    stream.write_meta("r");

    // for (auto n : all_nodes) {
    //     delete n;
    // }

    stack.free();
}

class CodeCPU : public ICodeCPU<STREAM>
{
public:
    bool run(Db &db, code_args_t &args, STREAM &stream) override
    {
        DbAccessor t(db);

        astar(t, args, stream);

        return t.commit();
    }

    ~CodeCPU() {}
};
}

extern "C" ICodeCPU<barrier::STREAM> *produce()
{
    // BARRIER!
    return new barrier::CodeCPU();
}

extern "C" void destruct(ICodeCPU<barrier::STREAM> *p) { delete p; }
