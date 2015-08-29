#ifndef MEMGRAPH_STORAGE_MODEL_PROPERTIES_STRING_HPP
#define MEMGRAPH_STORAGE_MODEL_PROPERTIES_STRING_HPP

#include "property.hpp"

class String : public Value<std::string>
{
public:
   using Value::Value;

   virtual void dump(std::string& buffer)
   {
       buffer += '"'; buffer += value; buffer += '"';
   }
};

#endif
