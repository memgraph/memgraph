/**
 * \file memgraph_op.cc
 * \brief Implementation of a memgraph operation in Tensorflow.
 */

#include <fmt/format.h>
#include <string>

#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/shape_inference.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/platform/default/logging.h"

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/value.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"

using namespace tensorflow;

const string kHost = "host";
const string kPort = "port";
const string kUser = "user";
const string kPassword = "password";
const string kUseSsl = "use_ssl";
const string kInputList = "input_list";

/**
 * @brief Construct attribute definition.
 * @details Attribute in TF has special format "key: value = default". This
 * function is helper function for constructing attribute definition.
 *
 * @param key Attribute name
 * @param value Attribute value
 * @param default_value Default value
 * @return "key: value = default_value"
 */
const string Define(const string& key, const string& value,
                    const string& default_value) {
  return key + ": " + value + " = " + default_value;
}

REGISTER_OP("MemgraphOp")
    .Attr(Define(kHost, "string", "'127.0.0.1'"))
    .Attr(Define(kPort, "int", "7687"))
    .Attr(Define(kUser, "string", "''"))
    .Attr(Define(kPassword, "string", "''"))
    .Attr(Define(kUseSsl, "bool", "false"))
    .Attr("output_dtype: {int64, double, bool, string}")
    .Input("query: string")
    .Input("input_list: int64")
    .Output("header: string")
    .Output("rows: output_dtype")
    .SetShapeFn([](::tensorflow::shape_inference::InferenceContext* c) {
      ::tensorflow::shape_inference::ShapeHandle input;
      TF_RETURN_IF_ERROR(c->WithRank(c->input(0), 0, &input));
      TF_RETURN_IF_ERROR(c->WithRank(c->input(1), 1, &input));
      return Status::OK();
    });

/**
 * @brief Memgraph Tensorflow Op
 * @details Memgraph op is the wrapper around the memgraph client. Memgraph op
 * takes attributes for connection information and one attribute for output type
 * definition: int64, double, bool, string. There are two inputs: query (string)
 * and the input list (int64 list). The user can use the input list in the query
 * with variable $input_list. There are two outputs: header and rows. Headers
 * are names of the columns in the output table and rows are all data fetch from
 * memgraph with the query. Memgraph Op has one limitation on output. All output
 * values in rows data must have the same type. If the user set output type to
 * string, then all data convert to the string,  however, in any other case
 * error appears  (the implicit cast is not possible for other types). List will
 * be expand into the matrix. All rows must contains lists with same size.
 *
 *
 * @tparam T Output type
 */
template <typename T>
class MemgraphOp final : public OpKernel {
 private:
  const string kBoltClientVersion = "TensorflowMemgraphOp";  // TODO version?
  string host_;
  int port_;
  string user_;
  string password_;
  bool use_ssl_;
  io::network::Endpoint endpoint_;
  communication::bolt::Client* client_;

  T GetValue(const communication::bolt::Value& value);

  string ToString(const communication::bolt::Value& value) {
    std::stringstream stream;
    stream << value;
    return stream.str();
  }

  string ToString(const communication::bolt::Value::Type& type) {
    std::stringstream stream;
    stream << type;
    return stream.str();
  }

 public:
  /**
   * @brief Constructor
   * @details Instance will connect to memgraph.
   *
   * @param context Context contains attribute values - database connection
   * information and output data type.
   */
  explicit MemgraphOp(OpKernelConstruction* context) : OpKernel(context) {
    OP_REQUIRES_OK(context, context->GetAttr(kHost, &host_));
    OP_REQUIRES_OK(context, context->GetAttr(kPort, &port_));
    OP_REQUIRES_OK(context, context->GetAttr(kUser, &user_));
    OP_REQUIRES_OK(context, context->GetAttr(kPassword, &password_));
    OP_REQUIRES_OK(context, context->GetAttr(kUseSsl, &use_ssl_));
    communication::Init();
    endpoint_ =
        io::network::Endpoint(io::network::ResolveHostname(host_), port_);
    communication::ClientContext context_mg(use_ssl_);
    client_ = new communication::bolt::Client(&context_mg);
    OP_REQUIRES(context, client_ != NULL,
                errors::Internal("Cannot create client instance"));
    try {
      client_->Connect(endpoint_, user_, password_, kBoltClientVersion);
    } catch (const communication::bolt::ClientFatalException& e) {
      OP_REQUIRES(context, false,
                  errors::Internal(
                      fmt::format("Cannot connect to memgraph: {}", e.what())));
    }
  }

  /**
   * @brief Compute stage
   * @details Op reads input data (query and input list), executes query and on
   * fills the outputs.
   *
   * @param context Context contains data for inputs and outputs.
   */
  void Compute(OpKernelContext* context) override {
    const Tensor& param_tensor = context->input(1);
    auto params = param_tensor.flat<int64>();
    std::vector<communication::bolt::Value> input_list;

    for (size_t i = 0; i < params.size(); ++i) {
      communication::bolt::Value value(static_cast<int64_t>(params(i)));
      input_list.push_back(value);
    }

    const Tensor& input_tensor = context->input(0);
    auto query = input_tensor.flat<string>()(0);

    string message;
    communication::bolt::QueryData ret;
    try {
      ret = client_->Execute(query, {{kInputList, input_list}});
    } catch (communication::bolt::ClientQueryException& e) {
      client_->Close();
      message = fmt::format("Query error: {}", e.what());
      OP_REQUIRES(context, false, errors::Internal(message));
    } catch (const communication::bolt::ClientFatalException& e) {
      client_->Close();
      message = fmt::format("Internal error: {}", e.what());
      bool is_connected = false;
      try {
        client_->Connect(endpoint_, user_, password_, kBoltClientVersion);
        is_connected = true;
      } catch (const communication::bolt::ClientFatalException& e) {
      }
      OP_REQUIRES(context, is_connected, errors::Internal(message));
    }

    // Use first row to find out Tensor size (inner lists).
    size_t row_width = 0;
    size_t row_height = 0;
    std::vector<size_t> column_list_size(ret.fields.size(), 0);
    if (ret.records.size() > 0) {
      for (size_t j = 0, i = 0; j < ret.records[i].size(); ++j) {
        const auto& field = ret.records[i][j];
        if (field.IsList())
          column_list_size[j] = field.ValueList().size();
        else
          column_list_size[j] = 1;
        row_width += column_list_size[j];
      }
    }

    if (ret.records.size() == 1 && row_width == 0)
      row_height = 0;
    else
      row_height = ret.records.size();

    // Create header output
    TensorShape header_output__shape;
    header_output__shape.AddDim(row_width);
    Tensor* header_output = NULL;
    OP_REQUIRES_OK(context, context->allocate_output(0, header_output__shape,
                                                     &header_output));
    auto header_output_flat = header_output->flat<string>();

    for (size_t i = 0, cnt = 0; i < ret.fields.size(); ++i)
      if (column_list_size[i] == 1)
        header_output_flat(cnt++) = ret.fields[i];
      else
        for (size_t j = 0; j < column_list_size[i]; ++j)
          header_output_flat(cnt++) = ret.fields[i] + "_" + std::to_string(j);

    // Create row output
    TensorShape rows_output_shape;
    rows_output_shape.AddDim(row_height);
    rows_output_shape.AddDim(row_width);
    Tensor* rows_output = NULL;
    OP_REQUIRES_OK(
        context, context->allocate_output(1, rows_output_shape, &rows_output));
    auto rows_output_matrix = rows_output->matrix<T>();

    for (size_t i = 0; i < row_height; ++i) {
      for (size_t j = 0, cnt = 0; j < ret.records[i].size(); ++j) {
        const auto& field = ret.records[i][j];
        try {
          if (field.IsList()) {
            OP_REQUIRES(context,
                        column_list_size[j] == field.ValueList().size(),
                        errors::Internal(fmt::format(
                            "List has wrong size, row: {}, header: {}", i,
                            ret.fields[j])));
            for (size_t k = 0; k < column_list_size[j]; ++k) {
              rows_output_matrix(i, cnt++) = GetValue(field.ValueList().at(k));
            }
          } else {
            rows_output_matrix(i, cnt++) = GetValue(field);
          }
        } catch (const communication::bolt::ValueException e) {
          string message =
              fmt::format("Wrong type: {} = {} ({})", header_output_flat(cnt),
                          field.type(), field);
          OP_REQUIRES(context, false, errors::Internal(message));
        }
      }
    }
  }
};

template <>
int64 MemgraphOp<int64>::GetValue(const communication::bolt::Value& value) {
  return value.ValueInt();
}

template <>
double MemgraphOp<double>::GetValue(const communication::bolt::Value& value) {
  return value.ValueDouble();
}

template <>
bool MemgraphOp<bool>::GetValue(const communication::bolt::Value& value) {
  return value.ValueBool();
}

template <>
string MemgraphOp<string>::GetValue(const communication::bolt::Value& value) {
  return ToString(value);
}

REGISTER_KERNEL_BUILDER(
    Name("MemgraphOp").Device(DEVICE_CPU).TypeConstraint<int64>("output_dtype"),
    MemgraphOp<int64>);
REGISTER_KERNEL_BUILDER(Name("MemgraphOp")
                            .Device(DEVICE_CPU)
                            .TypeConstraint<double>("output_dtype"),
                        MemgraphOp<double>);
REGISTER_KERNEL_BUILDER(
    Name("MemgraphOp").Device(DEVICE_CPU).TypeConstraint<bool>("output_dtype"),
    MemgraphOp<bool>);
REGISTER_KERNEL_BUILDER(Name("MemgraphOp")
                            .Device(DEVICE_CPU)
                            .TypeConstraint<string>("output_dtype"),
                        MemgraphOp<string>);
