#pragma once

#include <memory>

#include "data_structures/concurrent/concurrent_map.hpp"
#include "storage/indexes/index_holder.hpp"
#include "storage/model/properties/flags.hpp"
#include "storage/type_group_edge.hpp"
#include "storage/type_group_vertex.hpp"
#include "utils/option.hpp"
#include "utils/total_ordering.hpp"
#include "utils/underlying_cast.hpp"

// #include "storage/indexes/index_base.hpp"

// Family of properties with the same name but different types.
// Ordered on name.
// TG - group of types Edge/Vertex
template <class TG>
class PropertyFamily : public TotalOrdering<PropertyFamily<TG>>
{
    friend class PropertyType;
    friend class PropertyFamilyKey;
    friend class PropertyTypeKey;

public:
    // Type of property defined with his family and his type.
    // Ordered on PropertyFamily and Type.
    class PropertyType : public TotalOrdering<PropertyType>
    {
        friend class PropertyFamilyKey;
        friend class PropertyTypeKey;
        friend class PropertyFamily;

    public:
        // Ordered on POINTERS to PropertyFamily
        class PropertyFamilyKey
            : public TotalOrdering<PropertyFamilyKey>,
              public TotalOrdering<PropertyFamilyKey, PropertyFamily>,
              public TotalOrdering<PropertyFamily, PropertyFamilyKey>
        {
            friend class PropertyType;
            friend class PropertyTypeKey;

            PropertyFamilyKey(const PropertyType &type) : type(&type) {}

        public:
            friend bool operator==(const PropertyFamilyKey &lhs,
                                   const PropertyFamilyKey &rhs)
            {
                return &(lhs.type->family) == &(rhs.type->family);
            }

            friend bool operator<(const PropertyFamilyKey &lhs,
                                  const PropertyFamilyKey &rhs)
            {
                return &(lhs.type->family) < &(rhs.type->family);
            }

            friend bool operator==(const PropertyFamilyKey &lhs,
                                   const PropertyFamily &rhs)
            {
                return &(lhs.type->family) == &(rhs);
            }

            friend bool operator<(const PropertyFamilyKey &lhs,
                                  const PropertyFamily &rhs)
            {
                return &(lhs.type->family) < &(rhs);
            }

            friend bool operator==(const PropertyFamily &lhs,
                                   const PropertyFamilyKey &rhs)
            {
                return &(lhs) == &(rhs.type->family);
            }

            friend bool operator<(const PropertyFamily &lhs,
                                  const PropertyFamilyKey &rhs)
            {
                return &(lhs) < &(rhs.type->family);
            }

            Type get_type() const { return type->type; }

            Type prop_type() const { return type->type; }

            std::string const &family_name() const
            {
                return type->family.name();
            }

            const PropertyFamily &get_family() const { return type->family; }

            const std::string to_str() const
            {
                return std::string(family_name());
            }

        private:
            const PropertyType *type;
        };

        // Ordered on POINTERS to PropertyType.
        // When compared with PropertyFamilyKey behaves as PropertyFamilyKey.
        template <class T>
        class PropertyTypeKey : public TotalOrdering<PropertyTypeKey<T>>
        {
            friend class PropertyType;

            PropertyTypeKey(const PropertyType &type) : type(type) {}
        public:
            PropertyFamilyKey family_key() { return PropertyFamilyKey(type); }

            Type const &prop_type() const { return type.type; }

            friend bool operator==(const PropertyTypeKey &lhs,
                                   const PropertyTypeKey &rhs)
            {
                return &(lhs.type) == &(rhs.type);
            }

            friend bool operator<(const PropertyTypeKey &lhs,
                                  const PropertyTypeKey &rhs)
            {
                return &(lhs.type) < &(rhs.type);
            }

        private:
            const PropertyType &type;
        };

    private:
        PropertyType(PropertyFamily &family, Type type);
        PropertyType(PropertyFamily &other) = delete;
        PropertyType(PropertyFamily &&other) = delete;

    public:
        template <class T>
        bool is() const
        {
            return type == T::type;
        }

        bool is(Type &t) const;

        // Returns key ordered on POINTERS to PropertyType.
        // When compared with PropertyFamilyKey behaves as PropertyFamilyKey.
        template <class T>
        PropertyTypeKey<T> type_key()
        {
            assert(this->is<T>());
            return PropertyTypeKey<T>(*this);
        }

        // Returns key ordered on POINTERS to PropertyFamily
        PropertyFamilyKey family_key();

        friend bool operator<(const PropertyType &lhs, const PropertyType &rhs)
        {
            return lhs.family < rhs.family ||
                   (lhs.family == rhs.family && lhs.type < rhs.type);
        }

        friend bool operator==(const PropertyType &lhs, const PropertyType &rhs)
        {
            return lhs.family == rhs.family && lhs.type == rhs.type;
        }

    private:
        const PropertyFamily &family;
        const Type type;
    };

    PropertyFamily(std::string const &name_v);
    PropertyFamily(std::string &&name_v);
    PropertyFamily(PropertyFamily &other) = delete;
    PropertyFamily(PropertyFamily &&other) = delete;

    std::string const &name() const;

    // Returns type if it exists otherwise creates it.
    PropertyType &get(Type type);

    // Return pointer for NULL type. Extremly fast.
    PropertyType &getNull() const { return *null_type; }

    friend bool operator<(const PropertyFamily &lhs, const PropertyFamily &rhs);

    friend bool operator==(const PropertyFamily &lhs,
                           const PropertyFamily &rhs);

    IndexHolder<TG, std::nullptr_t> index;

private:
    const std::string name_v;
    // This is exclusivly for getNull method.
    PropertyType *null_type{nullptr};
    // TODO: Because types wont be removed this could be done with more efficent
    // data structure.
    ConcurrentMap<Type, std::unique_ptr<PropertyType>> types;
};

using VertexPropertyKey =
    PropertyFamily<TypeGroupVertex>::PropertyType::PropertyFamilyKey;
using EdgePropertyKey =
    PropertyFamily<TypeGroupEdge>::PropertyType::PropertyFamilyKey;
template <class T>
using VertexPropertyType =
    PropertyFamily<TypeGroupVertex>::PropertyType::PropertyTypeKey<T>;
template <class T>
using EdgePropertyType =
    PropertyFamily<TypeGroupEdge>::PropertyType::PropertyTypeKey<T>;
