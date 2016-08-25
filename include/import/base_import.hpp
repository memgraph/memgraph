#pragma once

#include <algorithm>
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

#include "import/element_skeleton.hpp"
#include "import/fillings/filler.hpp"
#include "storage/model/properties/flags.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/option.hpp"

using namespace std;

static Option<VertexAccessor> empty_op_vacc;

// Base importer with common facilities.
class BaseImporter
{

public:
    BaseImporter(DbAccessor &db, ostream &err_stream)
        : db(db), err_stream(err_stream)
    {
    }

    template <class... Args>
    void err(Args &... args)
    {
        if (error) {
            err_stream << " Error: ";
            out_err(args...);
            err_stream << endl;
        }
    }

    template <class... Args>
    void warn(Args &... args)
    {
        if (warning) {
            err_stream << "     Warning: ";
            out_err(args...);
            err_stream << endl;
        }
    }

    template <class T, class... Args>
    void out_err(T &first, Args &... args)
    {
        err_stream << first;
        out_err(args...);
    }

    template <class T>
    void out_err(T &first)
    {
        err_stream << first;
    }

    char *cstr(string &str) { return &str[0]; }

    bool split(string &str, char mark, vector<char *> &sub_str)
    {
        return split(cstr(str), mark, sub_str);
    }

    // Occurances of mark are changed with '\0'. sub_str is filled with
    // pointers to parts of str splited by mark in ascending order. Empty
    // sub_str are included. Doesn't split inside quotations and
    // open_bracket,closed_bracket.
    // Returns true if it was succesfully parsed.
    bool split(char *str, char mark, vector<char *> &sub_str)
    {

        int head = 0;
        bool in_text = false;
        bool in_array = false;

        for (int i = 0; str[i] != '\0'; i++) {
            char &c = str[i];

            // IN TEXT check
            if (c == quotations_mark) {
                in_text = !in_text;
                if (in_text && head == i) {
                    c = '\0';
                    head = i + 1;
                } else if (!in_text && !in_array) {
                    c = '\0';
                }
                continue;
            } else if (in_text) {
                continue;
            }

            // IN ARRAY check
            if (c == open_bracket) {
                if (in_array) {
                    err("Nested arrays aren't supported.");
                    return false;
                }
                in_array = true;
                continue;
            }
            if (in_array) {
                if (c == closed_bracket) {
                    in_array = false;
                }
                continue;
            }

            // SPLIT CHECK
            if (c == mark) {
                c = '\0';
                sub_str.push_back(&str[head]);
                head = i + 1;
            }
        }

        sub_str.push_back(&str[head]);

        return true;
    }

    // Extracts parts while stripping data of array chars and qutation marks.
    void extract(char *str, const char delimiter, vector<char *> &sub_str)
    {
        int head = 0;
        bool in_text = false;

        for (int i = 0; str[i] != '\0'; i++) {
            char &c = str[i];

            // IN TEXT check
            if (c == quotations_mark) {
                in_text = !in_text;
                if (in_text) {
                } else {
                    c = '\0';
                    sub_str.push_back(&str[head]);
                    head = i + 1;
                }
                head = i + 1;
                continue;
            } else if (in_text) {
                continue;
            }

            // IN ARRAY check
            if (c == open_bracket) {
                head = i + 1;
                continue;
            } else if (c == closed_bracket) {
                c = '\0';
                if (i > head) {
                    sub_str.push_back(&str[head]);
                }
                head = i + 1;
                continue;
            }

            // SPLIT CHECK
            if (c == delimiter) {
                c = '\0';
                if (i > head) {
                    sub_str.push_back(&str[head]);
                }
                head = i + 1;
            } else if (c == ' ' && i == head) {
                head++;
            }
        }

        sub_str.push_back(&str[head]);
        //
        // for (auto s : sub_str) {
        //     cout << "#" << s;
        // }
    }

    Option<VertexAccessor> const &get_vertex(size_t id)
    {
        if (vertices.size() > id) {
            return vertices[id];
        } else {
            cout << vertices.size() << " -> " << id << endl;
            return empty_op_vacc;
        }
    }

public:
    DbAccessor &db;

    char parts_mark = ',';
    char parts_array_mark = ',';
    char type_mark = ':';
    char quotations_mark = '"';
    char open_bracket = '[';
    char closed_bracket = ']';

    bool warning = true;
    bool error = true;

protected:
    // All errors are writen to this stream.
    ostream &err_stream;

    // All created vertices which have import local id
    vector<Option<VertexAccessor>> vertices;
};
