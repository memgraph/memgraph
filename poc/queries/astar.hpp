#include <iostream>
#include <queue>
#include <string>
#include <vector>

#include "query/i_plan_cpu.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/edge_x_vertex.hpp"
#include "utils/memory/stack_allocator.hpp"

using std::cout;
using std::endl;

// Dressipi astar query of 4 clicks.

// TODO: push down appropriate
using Stream = std::ostream;

// TODO: figure out from the pattern in a query
constexpr size_t max_depth = 3;

// TODO: from query LIMIT 10
constexpr size_t limit     = 10;

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
        auto now   = this;
        double sum = 0;
        do
        {
            sum += (now->vacc.at(tkey).get())->value();
            now = now->parent;
        } while (now != nullptr);
        return sum;
    }
};

bool vertex_filter_contained(DbAccessor &t, VertexAccessor &v, Node *before)
{
    if (v.fill())
    {
        bool found;
        do
        {
            found  = false;
            before = before->parent;
            if (before == nullptr)
            {
                return true;
            }
        } while (v.in_contains(before->vacc));
    }
    return false;
}

void astar(DbAccessor &t, plan_args_t &args, Stream &stream)
{
    StackAllocator stack;
    VertexPropertyType<Double> tkey = t.vertex_property_key<Double>("score");

    auto cmp = [](Node *left, Node *right) { return left->cost > right->cost; };
    std::priority_queue<Node *, std::vector<Node *>, decltype(cmp)> queue(cmp);

    // TODO: internal id independent
    auto start_vr = t.vertex_find(Id(args[0].as<Int64>().value()));
    if (!start_vr.is_present())
    {
        // TODO: stream failure

        return;
    }

    start_vr.get().fill();
    Node *start = new (stack.allocate<Node>()) Node(start_vr.take(), 0, tkey);
    queue.push(start);

    int count = 0;
    do
    {
        auto now = queue.top();
        queue.pop();

        if (now->depth >= max_depth)
        {
            // TODO: stream the result

            count++;

            if (count >= limit)
            {
                // the limit was reached -> STOP the execution
                break;
            }
            
            // if the limit wasn't reached -> POP the next vertex
            continue;
        }

        iter::for_all(now->vacc.out(), [&](auto edge) {
            VertexAccessor va = edge.to();
            if (vertex_filter_contained(t, va, now))
            {
                auto cost = 1 - va.at(tkey).get()->value();
                Node *n   = new (stack.allocate<Node>())
                    Node(va, now->cost + cost, now, tkey);
                queue.push(n);
            }
        });
    } while (!queue.empty());

    stack.free();
}

class PlanCPU : public IPlanCPU<Stream>
{
public:
    bool run(Db &db, plan_args_t &args, Stream &stream) override
    {
        DbAccessor t(db);

        // TODO: find node

        astar(t, args, stream);

        return t.commit();
    }

    ~PlanCPU() {}
};

extern "C" IPlanCPU<Stream> *produce() { return new PlanCPU(); }

extern "C" void destruct(IPlanCPU<Stream> *p) { delete p; }
