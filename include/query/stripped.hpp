#pragma once

#include "storage/model/properties/property.hpp"

/*
 * Query Plan Arguments Type
 */
using PlanArgsT = std::vector<Property>;

/*
* StrippedQuery contains:
*     * stripped query
*     * plan arguments stripped from query
*     * hash of stripped query
*
* @tparam THash a type of query hash
*/
template <typename THash>
struct StrippedQuery
{
   StrippedQuery(const std::string &&query, PlanArgsT &&arguments, THash hash)
       : query(std::forward<const std::string>(query)),
         arguments(std::forward<PlanArgsT>(arguments)), hash(hash)
   {
   }

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
   const PlanArgsT arguments;

   /**
    * Hash based on stripped query.
    */
   const THash hash;
};
