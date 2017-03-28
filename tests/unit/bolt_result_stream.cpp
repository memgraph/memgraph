#include "bolt_common.hpp"

#include "communication/bolt/v1/encoder/chunked_buffer.hpp"
#include "communication/bolt/v1/encoder/encoder.hpp"
#include "communication/bolt/v1/encoder/result_stream.hpp"
#include "query/backend/cpp/typed_value.hpp"

using BufferT = communication::bolt::ChunkedBuffer<TestSocket>;
using EncoderT = communication::bolt::Encoder<BufferT>;
using ResultStreamT = communication::bolt::ResultStream<EncoderT>;

/**
 * TODO (mferencevic): document
 */

const uint8_t header_output[] =
    "\x00\x29\xB1\x70\xA1\x86\x66\x69\x65\x6C\x64\x73\x9A\x82\x61\x61\x82\x62"
    "\x62\x82\x63\x63\x82\x64\x64\x82\x65\x65\x82\x66\x66\x82\x67\x67\x82\x68"
    "\x68\x82\x69\x69\x82\x6A\x6A\x00\x00";
const uint8_t result_output[] =
    "\x00\x0A\xB1\x71\x92\x05\x85\x68\x65\x6C\x6C\x6F\x00\x00";
const uint8_t summary_output[] =
    "\x00\x0C\xB1\x70\xA1\x87\x63\x68\x61\x6E\x67\x65\x64\x0A\x00\x00";

TEST(Bolt, ResultStream) {
  TestSocket socket(10);
  BufferT buffer(socket);
  EncoderT encoder(buffer);
  ResultStreamT result_stream(encoder);
  std::vector<uint8_t> &output = socket.output;

  std::vector<std::string> headers;
  for (int i = 0; i < 10; ++i)
    headers.push_back(std::string(2, (char)('a' + i)));

  result_stream.Header(headers); // this method automatically calls Flush
  PrintOutput(output);
  CheckOutput(output, header_output, 45);

  std::vector<TypedValue> result{TypedValue(5),
                                 TypedValue(std::string("hello"))};
  result_stream.Result(result);
  buffer.Flush();
  PrintOutput(output);
  CheckOutput(output, result_output, 14);

  std::map<std::string, TypedValue> summary;
  summary.insert(std::make_pair(std::string("changed"), TypedValue(10)));
  result_stream.Summary(summary);
  buffer.Flush();
  PrintOutput(output);
  CheckOutput(output, summary_output, 16);
}

int main(int argc, char **argv) {
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
