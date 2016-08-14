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
#include "storage/vertex_accessor.cpp"
#include "storage/vertex_accessor.hpp"

using namespace std;
typedef Vertex::Accessor VertexAccessor;
void load_graph_dummy(Db &db);
void load_csv(Db &db, char *file_path, char *edge_file_path);

class Node
{
public:
    Node *parent = {nullptr};
    double cost;
    int depth = {0};
    VertexAccessor vacc;

    Node(VertexAccessor vacc, double cost) : cost(cost), vacc(vacc) {}
    Node(VertexAccessor vacc, double cost, Node *parent)
        : cost(cost), vacc(vacc), parent(parent), depth(parent->depth + 1)
    {
    }
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

void found_result(Node *bef)
{
    std::cout << "{score: " << bef->cost << endl;
    while (bef != nullptr) {
        std::cout << "   " << *(bef->vacc.operator->()) << endl;
        bef = bef->parent;
    }
}

double calc_heuristic_cost_dummy(Edge::Accessor &edge, Vertex::Accessor &vertex)
{
    assert(!vertex.empty());
    return 1 - vertex->data.props.at("score").as<Double>().value;
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
void a_star(Db &db, int64_t sys_id_start, uint max_depth, EdgeFilter e_filter[],
            VertexFilter v_filter[],
            double (*calc_heuristic_cost)(Edge::Accessor &edge,
                                          Vertex::Accessor &vertex),
            int limit)
{
    DbAccessor t(db);

    auto cmp = [](Node *left, Node *right) { return left->cost > right->cost; };
    std::priority_queue<Node *, std::vector<Node *>, decltype(cmp)> queue(cmp);

    auto start_vr = t.vertex_find(sys_id_start);
    assert(start_vr);
    start_vr.get().fill();
    Node *start = new Node(start_vr.take(), 0);
    queue.push(start);
    int count = 0;
    do {
        auto now = queue.top();
        queue.pop();
        // if(!visited.insert(now)){
        //     continue;
        // }

        if (max_depth <= now->depth) {
            found_result(now);
            count++;
            if (count >= limit) {
                return;
            }
            continue;
        }

        iter::for_all(now->vacc.out(), [&](auto edge) {
            if (e_filter[now->depth](t, edge, now)) {
                VertexAccessor va = edge.to();
                if (v_filter[now->depth](t, va, now)) {
                    auto cost = calc_heuristic_cost(edge, va);
                    Node *n = new Node(va, now->cost + cost, now);
                    queue.push(n);
                }
            }
        });
    } while (!queue.empty());
    std::cout << "Found: " << count << " resoults\n";
    // TODO: GUBI SE MEMORIJA JER SE NODOVI NEBRISU

    t.commit();
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

int main()
{
    Db db;
    load_csv(db, "neo4j_nodes_export_2000.csv", "neo4j_edges_export_2000.csv");
    //
    // load_graph_dummy(db);
    //
    EdgeFilter e_filters[] = {&edge_filter_dummy, &edge_filter_dummy,
                              &edge_filter_dummy, &edge_filter_dummy};
    VertexFilter f_filters[] = {
        &vertex_filter_contained, &vertex_filter_contained,
        &vertex_filter_contained, &vertex_filter_contained};
    auto begin = clock();
    a_star(db, 0, 3, e_filters, f_filters, &calc_heuristic_cost_dummy, 10);
    clock_t end = clock();
    double elapsed_ms = (double(end - begin) / CLOCKS_PER_SEC) * 1000;
    std::cout << "A-star: " << elapsed_ms << " [ms]\n";

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

void load_csv(Db &db, char *file_path, char *edge_file_path)
{
    std::fstream file(file_path);
    std::fstream e_file(edge_file_path);

    std::string line;

    DbAccessor t(db);
    int max_score = 1000000;

    // VERTEX import
    int start_vertex_id = -1;
    auto v = [&](auto id, auto labels, auto gar_id, auto cat_id) {
        if (start_vertex_id < 0) {
            start_vertex_id = id;
        }

        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.property("id", std::make_shared<Int32>(id));
        vertex_accessor.property("garment_id", std::make_shared<Int32>(gar_id));
        vertex_accessor.property("garment_category_id",
                                 std::make_shared<Int32>(cat_id));
        std::srand(id ^ 0x7482616);
        vertex_accessor.property(
            "score", std::make_shared<Double>((std::rand() % max_score) /
                                              (max_score + 0.0)));
        for (auto l_name : labels) {
            auto &label = t.label_find_or_create(l_name);
            vertex_accessor.add_label(label);
        }

        return vertex_accessor;
    };

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
}

void load_graph_dummy(Db &db)
{
    DbAccessor t(db);
    auto v = [&](auto id, auto score) {
        auto vertex_accessor = t.vertex_insert();
        vertex_accessor.property("id", std::make_shared<Int32>(id));
        vertex_accessor.property("score", std::make_shared<Double>(score));
        return vertex_accessor.id();
    };

    Id va[] = {
        v(0, 0.5), v(1, 1), v(2, 0.3), v(3, 0.15), v(4, 0.8), v(5, 0.8),
    };

    auto e = [&](auto from, auto type, auto to) {
        auto v1 = t.vertex_find(va[from]);

        auto v2 = t.vertex_find(va[to]);

        auto edge_accessor = t.edge_insert(v1.get(), v2.get());

        auto &edge_type = t.type_find_or_create(type);
        edge_accessor.edge_type(edge_type);
    };

    e(0, "ok", 3);
    e(0, "ok", 2);
    e(0, "ok", 4);
    e(1, "ok", 3);
    e(2, "ok", 1);
    e(2, "ok", 4);
    e(3, "ok", 4);
    e(3, "ok", 5);
    e(4, "ok", 0);
    e(4, "ok", 1);
    e(5, "ok", 2);

    t.commit();
}
