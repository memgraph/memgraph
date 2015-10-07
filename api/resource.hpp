#ifndef MEMGRAPH_API_RESOURCE_HPP
#define MEMGRAPH_API_RESOURCE_HPP

#include <memory>
#include "speedy/speedy.hpp"
#include "utils/crtp.hpp"

namespace api
{

struct GET
{
    GET() = default;

    template <class T>
    void link(speedy::Speedy& app, const std::string& path, T& resource)
    {
        using namespace std::placeholders;
        app.get(path, std::bind(&T::get, resource, _1, _2));
    }
};

struct POST
{
    POST() = default;

    template <class T>
    void link(speedy::Speedy& app, const std::string& path, T& resource)
    {
        using namespace std::placeholders;
        app.post(path, std::bind(&T::post, resource, _1, _2));
    }
};

namespace detail
{

template <class T, class M>
struct Method : public M
{
    Method(speedy::Speedy& app, const std::string& path)
    {
        M::link(app, path, static_cast<T&>(*this));
    }
};

template <class T, class... Ms>
struct Methods;

template <class T, class M, class... Ms>
struct Methods<T, M, Ms...> : public Method<T, M>, public Methods<T, Ms...>
{
    Methods(speedy::Speedy& app, const std::string& path)
        : Method<T, M>(app, path), Methods<T, Ms...>(app, path) {}
};

template <class T, class M>
struct Methods<T, M> : public Method<T, M>
{
    using Method<T, M>::Method;
};

}

template <class T, class... Ms>
class Resource : public detail::Methods<T, Ms...>
{
public:
    Resource(speedy::Speedy& app, const std::string& path)
        : detail::Methods<T, Ms...>(app, path) {}
};

}

#endif
