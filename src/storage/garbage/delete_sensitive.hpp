#pragma once

// Base class for all classes which need to be safely disposed. Main usage is
// for garbage class operations.
class DeleteSensitive
{
public:
    virtual ~DeleteSensitive() {}
};
