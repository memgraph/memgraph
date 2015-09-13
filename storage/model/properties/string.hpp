#ifndef MEMGRAPH_STORAGE_MODEL_PROPERTIES_STRING_HPP
#define MEMGRAPH_STORAGE_MODEL_PROPERTIES_STRING_HPP

#include "property.hpp"

namespace model
{

class String : public Value<std::string>
{
public:
   using Value::Value;

   void dump(std::string& buffer) override
   {
       buffer += '"'; buffer += value; buffer += '"';
   }
};

}

#endif
