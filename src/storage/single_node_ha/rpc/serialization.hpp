#pragma once

#include "storage/common/types/property_value.hpp"
#include "storage/common/types/types.hpp"
#include "storage/single_node_ha/rpc/serialization.capnp.h"

namespace storage {

template <class Type>
void Save(const Common<Type> &common, capnp::Common::Builder *builder) {
  builder->setStorage(common.id_);
}

template <class Type>
void Load(Common<Type> *common, const capnp::Common::Reader &reader) {
  common->id_ = reader.getStorage();
}

void SaveCapnpPropertyValue(const PropertyValue &value,
                            capnp::PropertyValue::Builder *builder);

void LoadCapnpPropertyValue(const capnp::PropertyValue::Reader &reader,
                            PropertyValue *value);

}  // namespace storage
