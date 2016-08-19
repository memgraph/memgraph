#include <chrono>
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
#include "database/db_accessor.hpp"
#include "storage/edges.cpp"
#include "storage/edges.hpp"
#include "storage/indexes/impl/nonunique_unordered_index.cpp"
#include "storage/model/properties/properties.cpp"
#include "storage/record_accessor.cpp"
#include "storage/vertex_accessor.cpp"
#include "storage/vertex_accessor.hpp"
#include "storage/vertices.cpp"
#include "storage/vertices.hpp"

using namespace std;
typedef Vertex::Accessor VertexAccessor;
void load_graph_dummy(Db &db);
int load_csv(Db &db, char *file_path, char *edge_file_path);

class Node
{
public:
    Node *parent = {nullptr};
    type_key_t<Double> tkey;
    double cost;
    int depth = {0};
    VertexAccessor vacc;

    Node(VertexAccessor vacc, double cost, type_key_t<Double> tkey)
        : cost(cost), vacc(vacc), tkey(tkey)
    {
    }
    Node(VertexAccessor vacc, double cost, Node *parent,
         type_key_t<Double> tkey)
        : cost(cost), vacc(vacc), parent(parent), depth(parent->depth + 1),
          tkey(tkey)
    {
    }

    double sum_vertex_score()
    {
        auto now = this;
        double sum = 0;
        do {
            sum += *(now->vacc.at(tkey).get());
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

// class Iterator : public Crtp<Iterator>
// {
// public:
//     Vertex *operator*()
//     {
//         assert(head != nullptr);
//         return head->vertex;
//     }
//
//     Vertex *operator->()
//     {
//         assert(head != nullptr);
//         return head->vertex;
//     }
//
//     Iterator &operator++()
//     {
//         assert(head != nullptr);
//         head = head->parent;
//         return this->derived();
//     }
//
//     Iterator &operator++(int) { return operator++(); }
//
//     friend bool operator==(const Iterator &a, const Iterator &b)
//     {
//         return a.head == b.head;
//     }
//
//     friend bool operator!=(const Iterator &a, const Iterator &b)
//     {
//         return !(a == b);
//     }
//
//     Iterator end() { return Iterator(); }
//
// private:
//     Node *head;
// };

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

double calc_heuristic_cost_dummy(type_key_t<Double> tkey, Edge::Accessor &edge,
                                 Vertex::Accessor &vertex)
{
    assert(!vertex.empty());
    return 1 - *vertex.at(tkey).get();
}

typedef bool (*EdgeFilter)(DbAccessor &t, Edge::Accessor &, Node *before);
typedef bool (*VertexFilter)(DbAccessor &t, Vertex::Accessor &, Node *before);

bool edge_filter_dummy(DbAccessor &t, Edge::Accessor &e, Node *before)
{
    return true;
}

bool vertex_filter_dummy(DbAccessor &t, Vertex::Accessor &va, Node *before)
{
    return va.fill();
}

bool vertex_filter_contained_dummy(DbAccessor &t, Vertex::Accessor &v,
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

bool vertex_filter_contained(DbAccessor &t, Vertex::Accessor &v, Node *before)
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
auto a_star(Db &db, int64_t sys_id_start, uint max_depth, EdgeFilter e_filter[],
            VertexFilter v_filter[],
            double (*calc_heuristic_cost)(type_key_t<Double> tkey,
                                          Edge::Accessor &edge,
                                          Vertex::Accessor &vertex),
            int limit)
{
    DbAccessor t(db);
    type_key_t<Double> tkey = t.vertex_property_family_get("score")
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
    // std::cout << "Found: " << count << " resoults\n";
    // TODO: GUBI SE MEMORIJA JER SE NODOVI NEBRISU

    t.commit();
    return best;
}

// class Data
// {
//
// private:
//     size_t data = 0;
//     int key;
//
// public:
//     Data(int key) : key(key) {}
//
//     const int &get_key() { return key; }
// };

int main(int argc, char **argv)
{
    if (argc < 3) {
        std::cout << "Not enough input values\n";
        return 0;
    } else if (argc > 4) {
        std::cout << "To much input values\n";
        return 0;
    }

    Db db;
    auto vertex_no = load_csv(db, argv[argc - 2], argv[argc - 1]);

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
    bool pick_best_found = argc > 3 ? true : false;

    double sum = 0;
    std::vector<Node *> best;
    for (int i = 0; i < bench_n; i++) {
        auto start_vertex_index = std::rand() % vertex_no;

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

    // RhHashMultiMap benchmark
    // const int n_pow2 = 20;
    // int n = 1 << n_pow2;
    // RhHashMultiMap<int, Data, n_pow2 + 1> map;
    // std::srand(time(0));
    // auto data = std::vector<Data *>();
    // for (int i = 0; i < n; i++) {
    //     data.push_back(new Data(std::rand()));
    // }
    //
    // begin = clock();
    // for (auto e : data) {
    //     map.add(e);
    // }
    // end = clock();
    // elapsed_ms = (double(end - begin) / CLOCKS_PER_SEC) * 1000;
    // std::cout << "Map: " << elapsed_ms << " [ms]\n";

    return 0;
}

void split(const string &s, char delim, vector<string> &elems)
{
    stringstream ss(s);
    string item;
    while (getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

vector<string> split(const string &s, char delim)
{
    vector<string> elems;
    split(s, delim, elems);
    return elems;
}

int load_csv(Db &db, char *file_path, char *edge_file_path)
{
    std::fstream file(file_path);
    std::fstream e_file(edge_file_path);

    std::string line;

    DbAccessor t(db);
    auto key_id =
        t.vertex_property_family_get("id").get(Flags::Int32).family_key();
    auto key_garment_id = t.vertex_property_family_get("garment_id")
                              .get(Flags::Int32)
                              .family_key();
    auto key_garment_category_id =
        t.vertex_property_family_get("garment_category_id")
            .get(Flags::Int32)
            .family_key();
    auto key_score =
        t.vertex_property_family_get("score").get(Flags::Double).family_key();

    int max_score = 1000000;

    // VERTEX import
    int start_vertex_id = -1;
    auto v = [&](auto id, auto labels, auto gar_id, auto cat_id) {
        if (start_vertex_id < 0) {
            start_vertex_id = id;
        }

        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.set(key_id, std::make_shared<Int32>(id));
        vertex_accessor.set(key_garment_id, std::make_shared<Int32>(gar_id));
        vertex_accessor.set(key_garment_category_id,
                            std::make_shared<Int32>(cat_id));
        // from Kruno's head :) (could be ALMOST anything else)
        std::srand(id ^ 0x7482616);
        vertex_accessor.set(key_score,
                            std::make_shared<Double>((std::rand() % max_score) /
                                                     (max_score + 0.0)));

        for (auto l_name : labels) {
            auto &label = t.label_find_or_create(l_name);
            vertex_accessor.add_label(label);
        }

        return vertex_accessor;
    };

    // Skip header
    std::getline(file, line);

    vector<Vertex::Accessor> va;
    int v_count = 0;
    while (std::getline(file, line)) {
        v_count++;
        line.erase(std::remove(line.begin(), line.end(), '['), line.end());
        line.erase(std::remove(line.begin(), line.end(), ']'), line.end());
        line.erase(std::remove(line.begin(), line.end(), '\"'), line.end());
        line.erase(std::remove(line.begin(), line.end(), ' '), line.end());
        auto splited = split(line, ',');
        vector<string> labels(splited.begin() + 1,
                              splited.begin() + splited.size() - 2);
        auto vacs =
            v(stoi(splited[0]), labels, stoi(splited[splited.size() - 2]),
              stoi(splited[splited.size() - 1]));

        assert(va.size() == (uint64_t)vacs.id());
        va.push_back(vacs);
    }

    // EDGE IMPORT
    auto e = [&](auto from, auto type, auto to) {
        auto v1 = va[from - start_vertex_id];

        auto v2 = va[to - start_vertex_id];

        auto edge_accessor = t.edge_insert(v1, v2);

        auto &edge_type = t.type_find_or_create(type);
        edge_accessor.edge_type(edge_type);
    };

    std::getline(e_file, line);
    long count = 0;
    while (std::getline(e_file, line)) {
        auto splited = split(line, ',');
        count++;
        e(stoi(splited[2]), splited[1], stoi(splited[3]));
    }

    cout << "Loaded:\n   Vertices: " << v_count << "\n   Edges: " << count
         << endl;

    t.commit();
    return v_count;
}

void load_graph_dummy(Db &db)
{
    DbAccessor t(db);

    // TODO: update code
    // auto v = [&](auto id, auto score) {
    //     auto vertex_accessor = t.vertex_insert();
    //     vertex_accessor.property("id", std::make_shared<Int32>(id));
    //     vertex_accessor.property("score", std::make_shared<Double>(score));
    //     return vertex_accessor.id();
    // };
    //
    // Id va[] = {
    //     v(0, 0.5), v(1, 1), v(2, 0.3), v(3, 0.15), v(4, 0.8), v(5, 0.8),
    // };
    //
    // auto e = [&](auto from, auto type, auto to) {
    //     auto v1 = t.vertex_find(va[from]);
    //
    //     auto v2 = t.vertex_find(va[to]);
    //
    //     auto edge_accessor = t.edge_insert(v1.get(), v2.get());
    //
    //     auto &edge_type = t.type_find_or_create(type);
    //     edge_accessor.edge_type(edge_type);
    // };
    //
    // e(0, "ok", 3);
    // e(0, "ok", 2);
    // e(0, "ok", 4);
    // e(1, "ok", 3);
    // e(2, "ok", 1);
    // e(2, "ok", 4);
    // e(3, "ok", 4);
    // e(3, "ok", 5);
    // e(4, "ok", 0);
    // e(4, "ok", 1);
    // e(5, "ok", 2);

    t.commit();
}
