#include "database/db.hpp"

#include "storage/indexes/indexes.hpp"
#include "storage/model/properties/property_family.hpp"

Db::Db() = default;
Db::Db(const std::string &name) : name_(name) {}

std::string const &Db::name() const { return name_; }

Indexes Db::indexes() { return Indexes(*this); }

template <class TG, class I, class G>
bool Db::create_index_on_vertex_property_family(const char *name, G &coll,
                                                I &create_index)
{
    auto &family = coll.property_family_find_or_create(name);
    bool added_index = false;
    DbTransaction t(*this, tx_engine.begin([&](auto t) {
        added_index = family.index.set_index(create_index(t));
    }));

    if (added_index) {
        t.trans.wait_for_active();

        auto oindex = family.index.get_write(t.trans);
        if (oindex.is_present()) {
            auto index = oindex.get();

            for (auto &vr : coll.access()) {
                auto v = vr.second.find(t.trans);
                if (v != nullptr) {
                    if (!index->insert(IndexRecord<TG, std::nullptr_t>(
                            std::nullptr_t(), v, &vr.second))) {
                        // Index is probably unique.
                        auto owned_maybe = family.index.remove_index(index);
                        if (owned_maybe.is_present()) {
                            garbage.dispose(tx_engine.snapshot(),
                                            owned_maybe.get().release());
                        }

                        t.trans.abort();
                        return false;
                    }
                }
            }

            index->activate();
            t.trans.commit();
            return true;
        }
    }

    t.trans.abort();
    return false;
}

template <class TG, class K>
bool Db::remove_index(IndexHolder<TG, K> &ih)
{
    auto owned_maybe = ih.remove_index();
    if (owned_maybe.is_present()) {
        garbage.dispose(tx_engine.snapshot(), owned_maybe.get().release());
        return true;
    }

    return false;
}
