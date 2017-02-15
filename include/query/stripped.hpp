#pragma once

#include "storage/typed_value_store.hpp"
#include "utils/hashing/fnv.hpp"

/*
* StrippedQuery contains:
*     * stripped query
*     * plan arguments stripped from query
*     * hash of stripped query
*/
struct StrippedQuery {

   StrippedQuery(const std::string &&query, TypedValueStore<> &&arguments, HashType hash)
       : query(std::forward<const std::string>(query)),
         arguments(std::forward<TypedValueStore<>>(arguments)), hash(hash) {}

   /**
    * Copy constructor is deleted because we don't want to make unecessary
    * copies of this object (copying of string and vector could be expensive)
    */
   StrippedQuery(const StrippedQuery &other) = delete;
   StrippedQuery &operator=(const StrippedQuery &other) = delete;

   /**
    * Move is allowed operation because it is not expensive and we can
    * move the object after it was created.
    */
   StrippedQuery(StrippedQuery &&other) = default;
   StrippedQuery &operator=(StrippedQuery &&other) = default;

   /**
    * Stripped query
    */
   const std::string query;

   /**
    * Stripped arguments
    */
   const TypedValueStore<> arguments;

   /**
    * Hash based on stripped query.
    */
   const HashType hash;
};
