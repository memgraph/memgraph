#include <iostream>
#include <string>

#include "query/util.hpp"
#include "query/i_plan_cpu.hpp"
#include "storage/model/properties/all.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (g:garment {garment_id: 1234}) SET g:FF RETURN labels(g)
// Hash: 11123780635391515946

class CodeCPU : public IPlanCPU<Stream>
{
public:

    bool run(Db &db, plan_args_t &args, Stream &stream) override
    {
        DbAccessor t(db);

        indices_t indices = {{"garment_id", 0}};
        auto properties   = query_properties(indices, args);

        auto &label       = t.label_find_or_create("garment");
        
        stream.write_field("labels(g)");

        label.index()
            .for_range(t)
            .properties_filter(t, properties)
            .for_all([&](auto va) -> void {
                va.stream_repr(std::cout);
                auto &ff_label = t.label_find_or_create("FF");
                va.add_label(ff_label);
                auto &labels = va.labels();

                stream.write_record();
                stream.write_list_header(1);
                stream.write_list_header(labels.size());
                for (auto &label : labels)
                {
                    stream.write(label.get().str());
                }
                stream.chunk();
            });

        stream.write_meta(\"rw");

        return t.commit();
    }

    ~CodeCPU() {}
};

extern "C" IPlanCPU<Stream>* produce()
{
    return new CodeCPU();
}

extern "C" void destruct(IPlanCPU<Stream>* p)
{
    delete p;
}
