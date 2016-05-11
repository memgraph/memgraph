#pragma once

#include "api/restful/restful_resource.hpp"

#include "database/db.hpp"
#include "threading/task.hpp"
#include "speedy/speedy.hpp"


template <class T, class... Ms>
class Resource
{
public:
    /** @brief Resource constructor
     *
     *  List ALL the dependencies here so that ioc can resolve them and store
     *  them to protected members so that derived resources can access them
     *
     *  @param task shared_ptr to the task dispatching library
     *  @param db shared_ptr to the database instance
     */
    Resource(Task::sptr task, Db::sptr db)
        : task(task), db(db) {}

    /** @brief Link all resources to an instance of speedy
     * 
     *  link_resources.py generates an include.hpp file which includes and
     *  instantinates all available resources. The include.hpp file also calls
     *  this method to link the resources to speedy for a given path
     */
    void link(sp::Speedy& app, const std::string& path) 
    {
        // make sure this is called once even if someone actually calls this
        // function multiple times
        std::call_once(once_flag, [this, &app, &path]() {
            Restful<T, Ms...>(static_cast<T&>(*this), app, path);
        });
    }

protected:
    // all resources have pointers to these instances add everything else
    // neccessary here as a shared_ptr and also include it in the constructor
    // and modify the include.hpp.template to include the new dependencies for
    // resource linking
    Task::sptr task;
    Db::sptr db;

private:
    std::once_flag once_flag;
};
