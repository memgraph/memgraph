#include "isolation/header.hpp"

#include "isolation/db.hpp"

template <class TO, class FROM>
TO &ref_as(FROM &ref)
{
    return (*reinterpret_cast<TO *>(&ref));
}

template <class TO, class FROM>
TO value_as(FROM &&ref)
{
    return std::move((*reinterpret_cast<TO *>(&ref)));
}

sha::Accessor::Accessor(const sha::Accessor::Accessor &other)
    : Sized(sizeof(base::Accessor), alignof(base::Accessor))
{
    as<base::Accessor>() = other.as<base::Accessor>();
}

sha::Accessor::Accessor(sha::Accessor::Accessor &&other)
    : Sized(sizeof(base::Accessor), alignof(base::Accessor))
{
    as<base::Accessor>() = value_as<base::Accessor>(other);
}

sha::Accessor &sha::Accessor::operator=(const sha::Accessor::Accessor &other)
{
    // TODO
    return *this;
}
sha::Accessor &sha::Accessor::operator=(sha::Accessor::Accessor &&other)
{
    // TODO
    return *this;
}

int sha::Accessor::get_prop(sha::Name &name)
{
    return as<base::Accessor>().data;
}

sha::Accessor sha::Db::access()
{
    auto &db = as<base::Db>();
    db.accessed++;
    base::Accessor acc;
    acc.data = db.data;
    return sha::Accessor(value_as<sha::Accessor>(acc));
}

sha::Name &sha::Db::get_name(const char *str)
{
    auto &db = as<base::Db>();
    db.accessed++;
    return ref_as<sha::Name>(db.name);
}
