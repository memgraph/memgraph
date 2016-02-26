#pragma once

#include "code.hpp"
#include "cypher/visitor/traverser.hpp"

class UpdateTraverser : public Traverser, public Code
{
};
