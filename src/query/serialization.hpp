#pragma once

#include "query/context.hpp"
#include "query/serialization.capnp.h"
#include "query/typed_value.hpp"
#include "storage/serialization.hpp"

namespace distributed {
class DataManager;
}

namespace query {

void SaveCapnpTypedValue(const query::TypedValue &value,
                         capnp::TypedValue::Builder *builder,
                         storage::SendVersions versions, int worker_id);

void LoadCapnpTypedValue(const capnp::TypedValue::Reader &reader,
                         query::TypedValue *value,
                         database::GraphDbAccessor *dba,
                         distributed::DataManager *data_manager);

void SaveEvaluationContext(const EvaluationContext &ctx,
                           capnp::EvaluationContext::Builder *builder);

void LoadEvaluationContext(const capnp::EvaluationContext::Reader &reader,
                           EvaluationContext *ctx);

}  // namespace query
