#pragma once

#include <memory>

#include "speedy/speedy.hpp"
#include "utils/crtp.hpp"

#include "storage/model/properties/jsonwriter.hpp"

/** @brief GET handler method for the resource
 *  Contains the code for registering GET handler for a URL to Speedy
 */
struct GET
{
    /** @brief Links ::get handler to speedy for a given path
     *
     *  @tparam T Class type containing the required handler
     *  @param app Instance of speedy to register the method to
     *  @param path URL of the resource being registered
     *  @param resource Object containing ::get http::request_cb_t handler
     */
    template <class T>
    void link(sp::Speedy& app, const std::string& path, T& resource)
    {
        using namespace std::placeholders;
        app.get(path, std::bind(&T::get, std::ref(resource), _1, _2));
    }
};

/** @brief POST handler method for the resource
 *  Contains the code for registering POST handler for a URL to Speedy
 */
struct POST
{
    /** @brief Links ::post handler to speedy for a given path
     *
     *  @tparam T Class type containing the required handler
     *  @param app Instance of speedy to register the method to
     *  @param path URL of the resource being registered
     *  @param resource Object containing ::post http::request_cb_t handler
     */
    template <class T>
    void link(sp::Speedy& app, const std::string& path, T& resource)
    {
        using namespace std::placeholders;
        app.post(path, std::bind(&T::post, std::ref(resource), _1, _2));
    }
};

struct PUT
{
    template <class T>
    void link(sp::Speedy& app, const std::string& path, T& resource)
    {
        using namespace std::placeholders;
        app.put(path, std::bind(&T::put, std::ref(resource), _1, _2));
    }
};

struct DELETE
{
    template <class T>
    void link(sp::Speedy& app, const std::string& path, T& resource)
    {
        using namespace std::placeholders;
        app.del(path, std::bind(&T::del, std::ref(resource), _1, _2));
    }
};

namespace detail
{

/** @brief Registers a method for a path to speedy
 *
 *  @tparam T Derived class containing the handler of the method
 *  @tparam M A method to register
 */
template <class T, class M>
struct Method : public M
{
    /** Registers a route handler for the resource
     *
     *  Registers a method handler for M on the given URL
     *
     *  @param app instance of speedy to register the method to
     *  @param path URL of the resource being registered
     */
    Method(T& resource, sp::Speedy& app, const std::string& path)
    {
        M::link(app, path, resource);
    }
};

/** @brief Generates the Method<T, M> inheritance for each M in Ms
 * 
 *  Implemented inheriting recursively using variadic templates
 *
 *  @tparam T Derived class containing handlers for each method M in Ms
 *  @tparam Ms... Methods to register
 */
template <class T, class... Ms>
struct Methods;

/** @brief specialization of the struct Methods<T, Ms...>
 *
 *  Unrolls one method M and generates a handler for this method by inheriting
 *  from Method<T, M> and generates the rest of the handlers recursively by
 *  inheriting from Methods<T, Ms...> and unrolling one M each time.
 *
 *  @tparam T Derived class containing handlers for each method M in Ms
 *  @tparam M Unrolled method M for which to generate a handler
 *  @tparam Ms... The rest of the methods
 */
template <class T, class M, class... Ms>
struct Methods<T, M, Ms...> : public Method<T, M>, public Methods<T, Ms...>
{
    Methods(T& resource, sp::Speedy& app, const std::string& path)
        : Method<T, M>(resource, app, path),
          Methods<T, Ms...>(resource, app, path) {}
};

/** @brief specialization of the struct Methods<T, Ms...>
 *
 *  Specializes the recursion termination case containing only one method M
 *
 *  @tparam T Derived class containing handlers for method M
 *  @tparam M Unrolled method M for which to generate a handler
 */
template <class T, class M>
struct Methods<T, M> : public Method<T, M>
{
    using Method<T, M>::Method;
};

}

/** @brief Represents a restful resource
 *
 *  Automatically registers get, put, post, del... methods inside the derived
 *  class T. Methods are given as a template parameter to the class. Valid
 *  template parameters are classes which implement a function
 *
 *  void link(sp::Speedy&, const std::string&, T& resource)
 *
 *  which registers a method you want to use with speedy
 *
 *  @tparam T Derived class (CRTP)
 *  @tparam Ms... HTTP methods to register for this resource (GET, POST...)
 */
template <class T, class... Ms>
class Restful : public detail::Methods<T, Ms...>
{
public:
    Restful(T& resource, sp::Speedy& app, const std::string& path)
        : detail::Methods<T, Ms...>(resource, app, path) {}
};
