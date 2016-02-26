#pragma once

#include "code.hpp"
#include "cypher/visitor/traverser.hpp"

class DeleteTraverser : public Traverser, public Code
{
};
