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

using namespace std;

void load_graph_dummy(Db &db);
void load_csv(Db &db, char *file_path, char *edge_file_path);

class Node
{
public:
    Node *parent = {nullptr};
    double cost;
    int depth = {0};
    Vertex *vertex;
    VertexRecord *record;

    Node(Vertex *va, VertexRecord *record, double cost)
        : cost(cost), vertex(va), record(record)
    {
    }
    Node(Vertex *va, VertexRecord *record, double cost, Node *parent)
        : cost(cost), vertex(va), parent(parent), depth(parent->depth + 1),
          record(record)
    {
    }

    VertexRecord *&get_key() { return record; }
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
        std::cout << "   " << *(bef->vertex) << endl;
        bef = bef->parent;
    }
}

double calc_heuristic_cost_dummy(Edge *edge, Vertex *vertex)
{
    return 1 - vertex->data.props.at("score").as<Double>().value;
}

typedef bool (*EdgeFilter)(tx::Transaction &t, EdgeRecord *, Node *before);
typedef bool (*VertexFilter)(tx::Transaction &t, Vertex *, Node *before);

bool edge_filter_dummy(tx::Transaction &t, EdgeRecord *e, Node *before)
{
    return true;
}

bool vertex_filter_dummy(tx::Transaction &t, Vertex *v, Node *before)
{
    return true;
}

bool vertex_filter_contained_dummy(tx::Transaction &t, Vertex *v, Node *before)
{
    bool found;
    do {
        found = false;
        before = before->parent;
        if (before == nullptr) {
            return true;
        }
        for (auto edge : before->vertex->data.out) {
            Vertex *e_v = edge->to()->find(t);
            if (e_v == v) {
                found = true;
                break;
            }
        }
    } while (found);
    return false;
}

bool vertex_filter_contained(tx::Transaction &t, Vertex *v, Node *before)
{
    bool found;
    do {
        found = false;
        before = before->parent;
        if (before == nullptr) {
            return true;
        }
    } while (v->data.in.contains(before->record));
    return false;
}

// Vertex filter ima max_depth funkcija te edge filter ima max_depth funkcija.
// Jedan za svaku dubinu.
// Filtri vracaju true ako element zadovoljava uvjete.
void a_star(Db &db, int64_t sys_id_start, uint max_depth, EdgeFilter e_filter[],
            VertexFilter v_filter[],
            double (*calc_heuristic_cost)(Edge *edge, Vertex *vertex),
            int limit)
{
    auto &t = db.tx_engine.begin();
    RhHashMap<VertexRecord *, Node> visited;

    auto cmp = [](Node *left, Node *right) { return left->cost > right->cost; };
    std::priority_queue<Node *, std::vector<Node *>, decltype(cmp)> queue(cmp);

    auto start_vr = db.graph.vertices.find(t, sys_id_start).vlist;
    Node *start = new Node(start_vr->find(t), start_vr, 0);
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

        for (auto edge : now->vertex->data.out) {
            if (e_filter[now->depth](t, edge, now)) {
                Vertex *v = edge->to()->find(t);
                if (v_filter[now->depth](t, v, now)) {
                    Node *n = new Node(
                        v, edge->to(),
                        now->cost + calc_heuristic_cost(edge->find(t), v), now);
                    queue.push(n);
                }
            }
        }
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

    auto &t = db.tx_engine.begin();
    int max_score = 1000000;

    // VERTEX import
    int start_vertex_id = -1;
    auto v = [&](auto id, auto labels, auto gar_id, auto cat_id) {
        if (start_vertex_id < 0) {
            start_vertex_id = id;
        }

        auto vertex_accessor = db.graph.vertices.insert(t);
        vertex_accessor.property("id", std::make_shared<Int32>(id));
        vertex_accessor.property("garment_id", std::make_shared<Int32>(gar_id));
        vertex_accessor.property("garment_category_id",
                                 std::make_shared<Int32>(cat_id));
        std::srand(id ^ 0x7482616);
        vertex_accessor.property(
            "score", std::make_shared<Double>((std::rand() % max_score) /
                                              (max_score + 0.0)));
        for (auto l_name : labels) {
            auto &label = db.graph.label_store.find_or_create(l_name);
            vertex_accessor.add_label(label);
        }
        return vertex_accessor.id();
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
        auto id = v(stoi(splited[0]), labels, stoi(splited[splited.size() - 2]),
                    stoi(splited[splited.size() - 1]));

        assert(va.size() == (uint64_t)id);
        va.push_back(db.graph.vertices.find(t, id));
    }

    // EDGE IMPORT
    auto e = [&](auto from, auto type, auto to) {
        auto v1 = va[from - start_vertex_id];

        auto v2 = va[to - start_vertex_id];

        auto edge_accessor = db.graph.edges.insert(t, v1.vlist, v2.vlist);

        v1.vlist->update(t)->data.out.add(edge_accessor.vlist);
        v2.vlist->update(t)->data.in.add(edge_accessor.vlist);

        auto &edge_type = db.graph.edge_type_store.find_or_create(type);
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
    auto &t = db.tx_engine.begin();
    auto v = [&](auto id, auto score) {
        auto vertex_accessor = db.graph.vertices.insert(t);
        vertex_accessor.property("id", std::make_shared<Int32>(id));
        vertex_accessor.property("score", std::make_shared<Double>(score));
        return vertex_accessor.id();
    };

    Id va[] = {
        v(0, 0.5), v(1, 1), v(2, 0.3), v(3, 0.15), v(4, 0.8), v(5, 0.8),
    };

    auto e = [&](auto from, auto type, auto to) {
        auto v1 = db.graph.vertices.find(t, va[from]);

        auto v2 = db.graph.vertices.find(t, va[to]);

        auto edge_accessor = db.graph.edges.insert(t, v1.vlist, v2.vlist);
        v1.vlist->update(t)->data.out.add(edge_accessor.vlist);
        v2.vlist->update(t)->data.in.add(edge_accessor.vlist);

        auto &edge_type = db.graph.edge_type_store.find_or_create(type);
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
