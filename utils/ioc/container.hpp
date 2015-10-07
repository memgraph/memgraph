#ifndef MEMGRAPH_UTILS_IOC_CONTAINER_HPP
#define MEMGRAPH_UTILS_IOC_CONTAINER_HPP

#include <cassert>
#include <memory>
#include <vector>
#include <map>

namespace ioc
{

class Container
{
    struct Holdable
    {
        using uptr = std::unique_ptr<Holdable>;

        Holdable() = default;
        virtual ~Holdable() = default;
    };

    template <class T>
    struct Item : public Holdable
    {
        virtual std::shared_ptr<T> get() = 0;
    };

    template <class T>
    struct Instance : public Item<T>
    {
        Instance(std::shared_ptr<T> item) : item(std::move(item)) {}
        Instance(std::shared_ptr<T>&& item) : item(item) {}

        std::shared_ptr<T> get() override
        {
            assert(item != nullptr);
            return item;
        }

        std::shared_ptr<T> item;
    };

    template <class T>
    struct Creator : public Item<T>
    {
        using func = std::function<std::shared_ptr<T>()>;

        Creator(func&& f) : f(f) {}

        std::shared_ptr<T> get() override
        {
            return f();
        }

        func f;
    };

public:
    template <class T>
    std::shared_ptr<T> resolve()
    {
        auto it = items.find(key<T>());
        assert(it != items.end());

        // try to cast Holdable* to Item<T>*
        auto item = dynamic_cast<Item<T>*>(it->second.get());
        assert(item != nullptr);

        return item->get();
    }

    template <class T, class... Args>
    std::shared_ptr<T> singleton()
    {
        auto item = std::make_shared<T>(resolve<Args>()...);
        items.emplace(key<T>(), Holdable::uptr(new Instance<T>(item)));
        return item;
    }

    template <class T>
    void singleton(std::shared_ptr<T>&& item)
    {
        items.emplace(key<T>(), Holdable::uptr(new Instance<T>(item)));
    }

    template <class T>
    void factory(typename Creator<T>::func&& f)
   {
        items[key<T>()] = std::move(Holdable::uptr(
            new Creator<T>(std::forward<typename Creator<T>::func>(f))
        ));
    }

private:
    std::map<std::string, Holdable::uptr> items;

    template <class T>
    std::string key()
    {
        return typeid(T).name();
    }
};

}

#endif
