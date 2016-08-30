#include <chrono>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iostream>
#include <queue>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

#include "data_structures/map/rh_hashmap.hpp"
#include "database/db.hpp"
#include "database/db_accessor.cpp"
#include "database/db_accessor.hpp"
#include "import/csv_import.hpp"
#include "storage/edge_x_vertex.hpp"
#include "storage/edges.cpp"
#include "storage/edges.hpp"
#include "storage/indexes/impl/nonunique_unordered_index.cpp"
#include "storage/model/properties/properties.cpp"
#include "storage/record_accessor.cpp"
// #include "storage/vertex_accessor.cpp"
#include "storage/vertex_accessor.hpp"
#include "storage/vertices.cpp"
#include "storage/vertices.hpp"
#include "utils/command_line/arguments.hpp"
#include "communication/bolt/v1/serialization/bolt_serializer.hpp"

const int max_score = 1000000;

using namespace std;
typedef VertexAccessor VertexAccessor;

void add_scores(Db &db);

class Node
{
public:
    Node *parent = {nullptr};
    type_key_t<TypeGroupVertex, Double> tkey;
    double cost;
    int depth = {0};
    VertexAccessor vacc;

    Node(VertexAccessor vacc, double cost,
         type_key_t<TypeGroupVertex, Double> tkey)
        : cost(cost), vacc(vacc), tkey(tkey)
    {
    }
    Node(VertexAccessor vacc, double cost, Node *parent,
         type_key_t<TypeGroupVertex, Double> tkey)
        : cost(cost), vacc(vacc), parent(parent), depth(parent->depth + 1),
          tkey(tkey)
    {
    }

    double sum_vertex_score()
    {
        auto now = this;
        double sum = 0;
        do {
            sum += (now->vacc.at(tkey).get())->value;
            now = now->parent;
        } while (now != nullptr);
        return sum;
    }
};

class Score
{
public:
    Score() : value(std::numeric_limits<double>::max()) {}
    Score(double v) : value(v) {}
    double value;
};

void found_result(Node *res)
{
    double sum = res->sum_vertex_score();

    std::cout << "{score: " << sum << endl;
    auto bef = res;
    while (bef != nullptr) {
        std::cout << "   " << *(bef->vacc.operator->()) << endl;
        bef = bef->parent;
    }
}

double calc_heuristic_cost_dummy(type_key_t<TypeGroupVertex, Double> tkey,
                                 EdgeAccessor &edge, VertexAccessor &vertex)
{
    assert(!vertex.empty());
    return 1 - vertex.at(tkey).get()->value;
}

typedef bool (*EdgeFilter)(DbAccessor &t, EdgeAccessor &, Node *before);
typedef bool (*VertexFilter)(DbAccessor &t, VertexAccessor &, Node *before);

bool edge_filter_dummy(DbAccessor &t, EdgeAccessor &e, Node *before)
{
    return true;
}

bool vertex_filter_dummy(DbAccessor &t, VertexAccessor &va, Node *before)
{
    return va.fill();
}

bool vertex_filter_contained_dummy(DbAccessor &t, VertexAccessor &v,
                                   Node *before)
{
    if (v.fill()) {
        bool found;
        do {
            found = false;
            before = before->parent;
            if (before == nullptr) {
                return true;
            }
            auto it = before->vacc.out();
            for (auto e = it.next(); e.is_present(); e = it.next()) {
                VertexAccessor va = e.get().to();
                if (va == v) {
                    found = true;
                    break;
                }
            }
        } while (found);
    }
    return false;
}

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

// Vertex filter ima max_depth funkcija te edge filter ima max_depth funkcija.
// Jedan za svaku dubinu.
// Filtri vracaju true ako element zadovoljava uvjete.
auto a_star(
    Db &db, int64_t sys_id_start, uint max_depth, EdgeFilter e_filter[],
    VertexFilter v_filter[],
    double (*calc_heuristic_cost)(type_key_t<TypeGroupVertex, Double> tkey,
                                  EdgeAccessor &edge, VertexAccessor &vertex),
    int limit)
{
    DbAccessor t(db);
    type_key_t<TypeGroupVertex, Double> tkey =
        t.vertex_property_family_get("score")
            .get(Flags::Double)
            .type_key<Double>();

    auto best_found = new std::map<Id, Score>[max_depth];

    std::vector<Node *> best;
    auto cmp = [](Node *left, Node *right) { return left->cost > right->cost; };
    std::priority_queue<Node *, std::vector<Node *>, decltype(cmp)> queue(cmp);

    auto start_vr = t.vertex_find(sys_id_start);
    assert(start_vr);
    start_vr.get().fill();
    Node *start = new Node(start_vr.take(), 0, tkey);
    queue.push(start);
    int count = 0;
    do {
        auto now = queue.top();
        queue.pop();
        // if(!visited.insert(now)){
        //     continue;
        // }

        if (max_depth <= now->depth) {
            best.push_back(now);
            count++;
            if (count >= limit) {
                return best;
            }
            continue;
        }

        // { // FOUND FILTER
        //     Score &bef = best_found[now->depth][now->vacc.id()];
        //     if (bef.value <= now->cost) {
        //         continue;
        //     }
        //     bef.value = now->cost;
        // }

        iter::for_all(now->vacc.out(), [&](auto edge) {
            if (e_filter[now->depth](t, edge, now)) {
                VertexAccessor va = edge.to();
                if (v_filter[now->depth](t, va, now)) {
                    auto cost = calc_heuristic_cost(tkey, edge, va);
                    Node *n = new Node(va, now->cost + cost, now, tkey);
                    queue.push(n);
                }
            }
        });
    } while (!queue.empty());

    // TODO: GUBI SE MEMORIJA JER SE NODOVI NEBRISU

    t.commit();
    return best;
}

int main(int argc, char **argv)
{
    auto para = all_arguments(argc, argv);

    Db db;
    auto loaded = import_csv_from_arguments(db, para);
    add_scores(db);

    EdgeFilter e_filters[] = {&edge_filter_dummy, &edge_filter_dummy,
                              &edge_filter_dummy, &edge_filter_dummy};
    VertexFilter f_filters[] = {
        &vertex_filter_contained, &vertex_filter_contained,
        &vertex_filter_contained, &vertex_filter_contained};

    // CONF
    std::srand(time(0));
    auto best_n = 10;
    auto bench_n = 1000;
    auto best_print_n = 10;
    bool pick_best_found =
        strcmp(get_argument(para, "-p", "true").c_str(), "true") == 0;

    double sum = 0;
    std::vector<Node *> best;
    for (int i = 0; i < bench_n; i++) {
        auto start_vertex_index = std::rand() % loaded.first;

        auto begin = clock();
        auto found = a_star(db, start_vertex_index, 3, e_filters, f_filters,
                            &calc_heuristic_cost_dummy, best_n);
        clock_t end = clock();

        double elapsed_ms = (double(end - begin) / CLOCKS_PER_SEC) * 1000;
        sum += elapsed_ms;

        if ((best.size() < best_print_n && found.size() > best.size()) ||
            (pick_best_found && found.size() > 0 &&
             found.front()->sum_vertex_score() >
                 best.front()->sum_vertex_score())) {
            best = found;
        }

        // Just to be safe
        if (i + 1 == bench_n && best.size() == 0) {
            bench_n++;
        }
    }

    std::cout << "\nSearch for best " << best_n
              << " results has runing time of:\n    avg: " << sum / bench_n
              << " [ms]\n";
    std::cout << "\nExample of best result:\n";
    for (int i = 0; i < best_print_n && best.size() > 0; i++) {
        found_result(best.front());
        best.erase(best.begin());
    }

    return 0;
}

// Adds property score to all vertices.
void add_scores(Db &db)
{
    DbAccessor t(db);

    auto key_score =
        t.vertex_property_family_get("score").get(Flags::Double).family_key();

    int i = 1;
    iter::for_all(t.vertex_access(), [&](auto v) {
        if (v.fill()) {
            // from Kruno's head :) (could be ALMOST anything else)
            std::srand(i ^ 0x7482616);
            v.set(key_score,
                  std::make_shared<Double>((std::rand() % max_score) /
                                           (max_score + 0.0)));
            i++;
        }
    });

    t.commit();
}
