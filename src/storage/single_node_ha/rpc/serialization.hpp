#pragma once

#include "communication/rpc/serialization.hpp"
#include "storage/common/types/property_value.hpp"
#include "storage/common/types/slk.hpp"
#include "storage/common/types/types.hpp"
#include "storage/single_node_ha/rpc/serialization.capnp.h"

namespace storage {

void Save(const Label &label, capnp::Label::Builder *builder);

void Load(Label *label, const capnp::Label::Reader &reader);

void Save(const EdgeType &edge_type, capnp::EdgeType::Builder *builder);

void Load(EdgeType *edge_type, const capnp::EdgeType::Reader &reader);

void Save(const Property &property, capnp::Property::Builder *builder);

void Load(Property *property, const capnp::Property::Reader &reader);

void SaveCapnpPropertyValue(const PropertyValue &value,
                            capnp::PropertyValue::Builder *builder);

void LoadCapnpPropertyValue(const capnp::PropertyValue::Reader &reader,
                            PropertyValue *value);

}  // namespace storage
