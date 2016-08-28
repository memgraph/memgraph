#include "isolation/header.hpp"

#include "isolation/db.hpp"

template <class TO, class FROM>
TO &ref_as(FROM &ref)
{
    return (*reinterpret_cast<TO *>(&ref));
}

// template <class TO, class FROM>
// TO value_as(FROM &&ref)
// {
//     return std::move((*reinterpret_cast<TO *>(&ref)));
// }

sha::Accessor::Accessor(const sha::Accessor &other)
    : Sized(sizeof(::Accessor), alignof(::Accessor))
{
    as<::Accessor>() = other.as<::Accessor>();
}

sha::Accessor::Accessor(sha::Accessor &&other)
    : Sized(sizeof(::Accessor), alignof(::Accessor))
{
    as<::Accessor>() = value_as<::Accessor>(other);
}

sha::Accessor::~Accessor() { as<::Accessor>().~Accessor(); }

sha::Accessor &sha::Accessor::operator=(const sha::Accessor &other)
{
    // TODO
    return *this;
}
sha::Accessor &sha::Accessor::operator=(sha::Accessor &&other)
{
    // TODO
    return *this;
}

int sha::Accessor::get_prop(sha::Name &name) { return as<::Accessor>().data; }

sha::Accessor sha::Db::access()
{
    auto &db = as<::Db>();
    db.accessed++;
    ::Accessor acc;
    acc.data = db.data;
    return sha::Accessor(std::move(acc));
}

sha::Name &sha::Db::get_name(const char *str)
{
    auto &db = as<::Db>();
    db.accessed++;
    return ref_as<sha::Name>(db.name);
}
