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

#include "import/base_import.hpp"
#include "import/element_skeleton.hpp"
#include "import/fillings/array.hpp"
#include "import/fillings/bool.hpp"
#include "import/fillings/double.hpp"
#include "import/fillings/filler.hpp"
#include "import/fillings/float.hpp"
#include "import/fillings/from.hpp"
#include "import/fillings/id.hpp"
#include "import/fillings/int32.hpp"
#include "import/fillings/int64.hpp"
#include "import/fillings/label.hpp"
#include "import/fillings/skip.hpp"
#include "import/fillings/string.hpp"
#include "import/fillings/to.hpp"
#include "import/fillings/type.hpp"
#include "storage/model/properties/all.hpp"
#include "storage/model/properties/flags.hpp"
#include "storage/vertex_accessor.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/option.hpp"

using namespace std;

constexpr char const *_string = "string";

// Will change all int32 into int64, and all float into double from csv into
// database. Uplifting will occure even in arrays.
constexpr const bool UPLIFT_PRIMITIVES = true;

bool equal_str(const char *a, const char *b) { return strcasecmp(a, b) == 0; }

// CSV importer for importing multiple files regarding same graph.
// CSV format of file should be following:
// header
// line of data
// line of data
// ...
//
// Where header should be composed of parts splited by parts_mark. Number of
// parts should be same as number of parts in every line of data. Parts should
// be of format name:type where name is alfanumeric identifyer of data in thath
// column and type should be one of: id, from, to, label, type, bool, int, long,
// float, double, string, bool[], int[], long[], float[], double[], string[].
// If name is missing the column data wont be saved into the elements.
// if the type is missing the column will be interperted as type string. If
// neither name nor type are present column will be skipped.
class CSVImporter : public BaseImporter
{

public:
    CSVImporter(DbAccessor &db)
        : BaseImporter(db, logging::log->logger("CSV_import"))
    {
    }

    // Loads data from stream and returns number of loaded vertexes.
    size_t import_vertices(std::fstream &file)
    {
        return import<TypeGroupVertex>(file, create_vertex, true);
    }

    // Loads data from stream and returns number of loaded edges.
    size_t import_edges(std::fstream &file)
    {
        return import<TypeGroupEdge>(file, create_edge, false);
    }

private:
    // Loads data from file and returns number of loaded name.
    // TG - TypeGroup
    // F - function which will create element from filled element skelleton.
    template <class TG, class F>
    size_t import(std::fstream &file, F f, bool vertex)
    {
        string line;
        vector<char *> sub_str;
        vector<unique_ptr<Filler>> fillers;
        vector<char *> tmp;

        // HEADERS
        if (!getline(file, line)) {
            logger.error("No lines");
            return 0;
        }

        if (!split(line, parts_mark, sub_str)) {
            logger.error("Illegal headers");
            return 0;
        }

        for (auto p : sub_str) {
            auto o = get_filler<TG>(p, tmp, vertex);
            if (o.is_present()) {
                fillers.push_back(o.take());
            } else {
                return 0;
            }
        }
        sub_str.clear();

        // LOAD DATA LINES
        size_t count = 0;
        size_t line_no = 1;
        ElementSkeleton es(db);
        while (std::getline(file, line)) {
            sub_str.clear();
            es.clear();

            if (split(line, parts_mark, sub_str)) {
                check_for_part_count(sub_str.size() - fillers.size(), line_no);

                int n = min(sub_str.size(), fillers.size());
                for (int i = 0; i < n; i++) {
                    auto er = fillers[i]->fill(es, sub_str[i]);
                    if (er.is_present()) {
                        logger.error("{} on line: {}", er.get(), line_no);
                    }
                }

                if (f(this, es, line_no)) {
                    count++;
                }
            }

            line_no++;
        }

        return count;
    }

    static bool create_vertex(CSVImporter *im, ElementSkeleton &es,
                              size_t line_no)
    {
        auto va = es.add_vertex();
        auto id = es.element_id();
        if (id.is_present()) {

            if (im->vertices.size() <= id.get()) {
                Option<VertexAccessor> empty = make_option<VertexAccessor>();
                im->vertices.insert(im->vertices.end(),
                                    id.get() - im->vertices.size() + 1, empty);
            }
            if (im->vertices[id.get()].is_present()) {
                im->logger.error("Vertex on line: {} has same id with another "
                                 "previously loaded vertex",
                                 line_no);
                return false;
            } else {
                im->vertices[id.get()] = make_option(std::move(va));
                return true;
            }
        } else {
            im->logger.warn("Missing import local vertex id for vertex on "
                            "line: {}",
                            line_no);
        }

        return true;
    }

    static bool create_edge(CSVImporter *im, ElementSkeleton &es,
                            size_t line_no)
    {
        auto o = es.add_edge();
        if (!o.is_present()) {
            return true;
        } else {
            im->logger.error("{} on line: {}", o.get(), line_no);
            return false;
        }
    }

    template <class TG>
    typename PropertyFamily<TG>::PropertyType::PropertyFamilyKey
    property_key(const char *name, Flags type)
    {
        assert(false);
    }

    // Returns filler for name:type in header_part. None if error occured.
    template <class TG>
    Option<unique_ptr<Filler>> get_filler(char *header_part,
                                          vector<char *> &tmp_vec, bool vertex)
    {
        tmp_vec.clear();
        split(header_part, type_mark, tmp_vec);

        const char *name = tmp_vec[0];
        const char *type = tmp_vec[1];

        if (tmp_vec.size() > 2) {
            logger.error("To much sub parts in header part");
            return make_option<unique_ptr<Filler>>();

        } else if (tmp_vec.size() < 2) {
            if (tmp_vec.size() == 1) {
                logger.warn("Column: {} doesn't have specified type so string "
                            "type will be used",
                            tmp_vec[0]);
                name = tmp_vec[0];
                type = _string;

            } else {
                logger.warn("Empty colum definition, skiping column.");
                std::unique_ptr<Filler> f(new SkipFiller());
                return make_option(std::move(f));
            }

        } else {
            name = tmp_vec[0];
            type = tmp_vec[1];
        }

        // Create adequat filler
        if (equal_str(type, "id")) {
            std::unique_ptr<Filler> f(
                name[0] == '\0' ? new IdFiller<TG>()
                                : new IdFiller<TG>(make_option(
                                      property_key<TG>(name, Flags::Int64))));
            return make_option(std::move(f));

        } else if (equal_str(type, "start_id") || equal_str(type, "from_id") ||
                   equal_str(type, "from") || equal_str(type, "source")) {
            std::unique_ptr<Filler> f(new FromFiller(*this));
            return make_option(std::move(f));

        } else if (equal_str(type, "label")) {
            std::unique_ptr<Filler> f(new LabelFiller(*this));
            return make_option(std::move(f));

        } else if (equal_str(type, "end_id") || equal_str(type, "to_id") ||
                   equal_str(type, "to") || equal_str(type, "target")) {
            std::unique_ptr<Filler> f(new ToFiller(*this));
            return make_option(std::move(f));

        } else if (equal_str(type, "type")) {
            std::unique_ptr<Filler> f(new TypeFiller(*this));
            return make_option(std::move(f));

        } else if (name[0] == '\0') { // OTHER FILLERS REQUIRE NAME
            logger.warn("Unnamed column of type: {} will be skipped.", type);
            std::unique_ptr<Filler> f(new SkipFiller());
            return make_option(std::move(f));

            // *********************** PROPERTIES
        } else if (equal_str(type, "bool")) {
            std::unique_ptr<Filler> f(
                new BoolFiller<TG>(property_key<TG>(name, Flags::Bool)));
            return make_option(std::move(f));

        } else if (equal_str(type, "double") ||
                   (UPLIFT_PRIMITIVES && equal_str(type, "float"))) {
            std::unique_ptr<Filler> f(
                new DoubleFiller<TG>(property_key<TG>(name, Flags::Double)));
            return make_option(std::move(f));

        } else if (equal_str(type, "float")) {
            std::unique_ptr<Filler> f(
                new FloatFiller<TG>(property_key<TG>(name, Flags::Float)));
            return make_option(std::move(f));

        } else if (equal_str(type, "long") ||
                   (UPLIFT_PRIMITIVES && equal_str(type, "int"))) {
            std::unique_ptr<Filler> f(
                new Int64Filler<TG>(property_key<TG>(name, Flags::Int64)));
            return make_option(std::move(f));

        } else if (equal_str(type, "int")) {
            std::unique_ptr<Filler> f(
                new Int32Filler<TG>(property_key<TG>(name, Flags::Int32)));
            return make_option(std::move(f));

        } else if (equal_str(type, "string")) {
            std::unique_ptr<Filler> f(
                new StringFiller<TG>(property_key<TG>(name, Flags::String)));
            return make_option(std::move(f));

        } else if (equal_str(type, "bool[]")) {
            std::unique_ptr<Filler> f(make_array_filler<TG, bool, ArrayBool>(
                *this, property_key<TG>(name, Flags::ArrayBool), to_bool));
            return make_option(std::move(f));

        } else if (equal_str(type, "double[]") ||
                   (UPLIFT_PRIMITIVES && equal_str(type, "float[]"))) {
            std::unique_ptr<Filler> f(
                make_array_filler<TG, double, ArrayDouble>(
                    *this, property_key<TG>(name, Flags::ArrayDouble),
                    to_double));
            return make_option(std::move(f));

        } else if (equal_str(type, "float[]")) {
            std::unique_ptr<Filler> f(make_array_filler<TG, float, ArrayFloat>(
                *this, property_key<TG>(name, Flags::ArrayFloat), to_float));
            return make_option(std::move(f));

        } else if (equal_str(type, "long[]") ||
                   (UPLIFT_PRIMITIVES && equal_str(type, "int[]"))) {
            std::unique_ptr<Filler> f(
                make_array_filler<TG, int64_t, ArrayInt64>(
                    *this, property_key<TG>(name, Flags::ArrayInt64),
                    to_int64));
            return make_option(std::move(f));

        } else if (equal_str(type, "int[]")) {
            std::unique_ptr<Filler> f(
                make_array_filler<TG, int32_t, ArrayInt32>(
                    *this, property_key<TG>(name, Flags::ArrayInt32),
                    to_int32));
            return make_option(std::move(f));

        } else if (equal_str(type, "string[]")) {
            std::unique_ptr<Filler> f(
                make_array_filler<TG, std::string, ArrayString>(
                    *this, property_key<TG>(name, Flags::ArrayString),
                    to_string));
            return make_option(std::move(f));

        } else {
            logger.error("Unknown type: {}", type);
            return make_option<unique_ptr<Filler>>();
        }
    }

    void check_for_part_count(long diff, long line_no)
    {
        if (diff != 0) {
            if (diff < 0) {
                logger.warn("Line no: {} has less parts then specified in "
                            "header. Missing: {} parts",
                            line_no, diff);
            } else {
                logger.warn("Line no: {} has more parts then specified in "
                            "header. Extra: {} parts",
                            line_no, diff);
            }
        }
    }
};

template <>
PropertyFamily<TypeGroupVertex>::PropertyType::PropertyFamilyKey
CSVImporter::property_key<TypeGroupVertex>(const char *name, Flags type)
{
    return db.vertex_property_key(name, Type(type));
}

template <>
PropertyFamily<TypeGroupEdge>::PropertyType::PropertyFamilyKey
CSVImporter::property_key<TypeGroupEdge>(const char *name, Flags type)
{
    return db.edge_property_key(name, Type(type));
}

// Imports all -v "vertex_file_path.csv" vertices and -e "edge_file_path.csv"
// edges from specified files. Also defines arguments -d, -ad.
// -d delimiter => sets delimiter for parsing .csv files. Default is ,
// -ad delimiter => sets delimiter for parsing arrays in .csv. Default is
// Returns (no loaded vertices,no loaded edges)
std::pair<size_t, size_t>
import_csv_from_arguments(Db &db, std::vector<std::string> &para)
{
    DbAccessor t(db);
    CSVImporter imp(t);

    imp.parts_mark = get_argument(para, "-d", ",")[0];
    imp.parts_array_mark = get_argument(para, "-ad", ",")[0];

    // IMPORT VERTICES
    size_t l_v = 0;
    auto o = take_argument(para, "-v");
    while (o.is_present()) {
        std::fstream file(o.get());

        imp.logger.info("Importing vertices from file: {}", o.get());

        auto n = imp.import_vertices(file);
        l_v = +n;

        imp.logger.info("Loaded: {} vertices from {}", n, o.get());

        o = take_argument(para, "-v");
    }

    // IMPORT EDGES
    size_t l_e = 0;
    o = take_argument(para, "-e");
    while (o.is_present()) {
        std::fstream file(o.get());

        imp.logger.info("Importing edges from file: {}", o.get());

        auto n = imp.import_edges(file);
        l_e = +n;

        imp.logger.info("Loaded: {} edges from {}", n, o.get());

        o = take_argument(para, "-e");
    }

    t.commit();

    return std::make_pair(l_v, l_e);
}
