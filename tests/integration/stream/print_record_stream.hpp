#pragma once

#include <string>
#include <vector>
#include <map>

#include "utils/exceptions/not_yet_implemented.hpp"

class PrintRecordStream
{
private:
    std::ostream& stream;

public:
    PrintRecordStream(std::ostream &stream) : stream(stream) {}

    void write_success()
    {
        stream << "SUCCESS\n"; 
    }

    void write_success_empty()
    {
        stream << "SUCCESS EMPTY\n";
    }

    void write_ignored()
    {
        stream << "IGNORED\n"; 
    }

    void write_empty_fields()
    {
        stream << "EMPTY FIELDS\n"; 
    }

    void write_fields(const std::vector<std::string> &fields)
    {
        stream << "FIELDS:";
        for (auto &field : fields)
        {
            stream << " " << field;
        }
        stream << '\n';
    }

    void write_field(const std::string &field)
    {
        stream << "Field: " << field << '\n';
    }

    void write_list_header(size_t size)
    {
        stream << "List: " << size << '\n';
    }

    void write_record()
    {
        stream << "Record\n";
    }

    void write_meta(const std::string &type)
    {
        stream << "Meta: " << type;
    }

    void write_failure(const std::map<std::string, std::string> &data)
    {
        throw NotYetImplemented();
    }

    void write_count(const size_t count)
    {
        throw NotYetImplemented();
    }

    void write(const VertexAccessor &vertex)
    {
        throw NotYetImplemented();
    }

    void write_vertex_record(const VertexAccessor& va)
    {
        throw NotYetImplemented();
    }

    void write(const EdgeAccessor &edge)
    {
        throw NotYetImplemented();
    }

    void write_edge_record(const EdgeAccessor& ea)
    {
        throw NotYetImplemented();
    }

    void write(const StoredProperty<TypeGroupEdge> &prop)
    {
    //    prop.accept(serializer);
        throw NotYetImplemented();
    }

    void write(const StoredProperty<TypeGroupVertex> &prop)
    {
    //   prop.accept(serializer);
        throw NotYetImplemented();
    }

    void write(const Null &prop)
    {
        throw NotYetImplemented();
    }

    void write(const Bool &prop)
    {
        throw NotYetImplemented();
    }

    void write(const Float &prop) { throw NotYetImplemented(); }
    void write(const Int32 &prop) { throw NotYetImplemented(); }
    void write(const Int64 &prop) { throw NotYetImplemented(); }
    void write(const Double &prop) { throw NotYetImplemented(); }
    void write(const String &prop) { throw NotYetImplemented(); }
    void write(const ArrayBool &prop) { throw NotYetImplemented(); }
    void write(const ArrayInt32 &prop) { throw NotYetImplemented(); }
    void write(const ArrayInt64 &prop) { throw NotYetImplemented(); }
    void write(const ArrayFloat &prop) { throw NotYetImplemented(); }
    void write(const ArrayDouble &prop) { throw NotYetImplemented(); }
    void write(const ArrayString &prop) { throw NotYetImplemented(); }

    void send()
    {
        throw NotYetImplemented();
    }

    void chunk()
    { 
        throw NotYetImplemented();
    }
};
