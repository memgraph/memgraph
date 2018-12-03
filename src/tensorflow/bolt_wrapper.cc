/// \file bolt_wrapper.cc
/// \brief Implementation of a bolt wrapper.
/// operation in Tensorflow.

#include <iostream>
#include <string>

#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/shape_inference.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/platform/default/logging.h"

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/utils.hpp"

using namespace tensorflow;

const string kHost = "host";
const string kPort = "port";
const string kUser = "user";
const string kPassword = "password";
const string kUseSsl = "use_ssl";

const string Define(const string& key, const string& value,
                    const string& defult) {
  return key + ": " + value + " = " + defult;
}

REGISTER_OP("BoltWrapper")
    .Attr(Define(kHost, "string", "'127.0.0.1'"))
    .Attr(Define(kPort, "int", "7687"))
    .Attr(Define(kUser, "string", "''"))
    .Attr(Define(kPassword, "string", "''"))
    .Attr(Define(kUseSsl, "bool", "false"))
    .Input("query: string")
    .Output("header: string")
    .Output("rows: string")
    .SetShapeFn([](::tensorflow::shape_inference::InferenceContext* c) {
      ::tensorflow::shape_inference::ShapeHandle input;
      TF_RETURN_IF_ERROR(c->WithRank(c->input(0), 0, &input));
      return Status::OK();
    });

class BoltWrapperOp : public OpKernel {
 private:
  const string kBoltClientVersion =
      "TensorflowClient";  // TODO maybe we can use real version...
  string host_;
  int port_;
  string user_;
  string password_;
  bool use_ssl_;
  communication::bolt::Client* client_;

 public:
  /// \brief Constructor.
  /// \param context
  explicit BoltWrapperOp(OpKernelConstruction* context) : OpKernel(context) {
    OP_REQUIRES_OK(context, context->GetAttr(kHost, &host_));
    OP_REQUIRES_OK(context, context->GetAttr(kPort, &port_));
    OP_REQUIRES_OK(context, context->GetAttr(kUser, &user_));
    OP_REQUIRES_OK(context, context->GetAttr(kPassword, &password_));
    OP_REQUIRES_OK(context, context->GetAttr(kUseSsl, &use_ssl_));
    communication::Init();
    io::network::Endpoint endpoint(io::network::ResolveHostname(host_), port_);
    communication::ClientContext context_mg(use_ssl_);
    client_ = new communication::bolt::Client(&context_mg);
    OP_REQUIRES(context, client_ != NULL,
                errors::Internal("Cannot create client"));

    client_->Connect(endpoint, user_, password_, kBoltClientVersion);
  }

  /// \brief Compute the inner product.
  /// \param context
  void Compute(OpKernelContext* context) override {
    bool exception_free = true;
    string message;
    try {
      const Tensor& input_tensor = context->input(0);
      auto query = input_tensor.flat<string>()(0);

      auto ret = client_->Execute(query, {});

      TensorShape header_output__shape;
      header_output__shape.AddDim(ret.fields.size());
      Tensor* header_output = NULL;
      OP_REQUIRES_OK(context, context->allocate_output(0, header_output__shape,
                                                       &header_output));
      auto header_output_flat = header_output->flat<string>();

      TensorShape rows_output_shape;
      rows_output_shape.AddDim(ret.records.size());
      rows_output_shape.AddDim(ret.fields.size());
      Tensor* rows_output = NULL;
      OP_REQUIRES_OK(context, context->allocate_output(1, rows_output_shape,
                                                       &rows_output));
      auto rows_output_matrix = rows_output->matrix<string>();

      for (int i = 0; i < ret.fields.size(); ++i) {
        header_output_flat(i) = ret.fields[i];
      }

      for (int i = 0; i < ret.records.size(); ++i) {
        for (int j = 0; j < ret.records[i].size(); ++j) {
          const auto& field = ret.records[i][j];
          std::stringstream field_stream;
          field_stream << field;
          rows_output_matrix(i, j) = field_stream.str();
        }
      }
    } catch (const communication::bolt::ClientFatalException& e) {
      client_->Close();
      exception_free = false;
      message = e.what();
    }
    OP_REQUIRES(context, exception_free, errors::Internal(message));
  }
};

REGISTER_KERNEL_BUILDER(Name("BoltWrapper").Device(DEVICE_CPU), BoltWrapperOp);
