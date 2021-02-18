#include "bolt_common.hpp"
#include "bolt_testdata.hpp"

#include "communication/bolt/v1/encoder/encoder.hpp"
#include "glue/communication.hpp"
#include "storage/v2/storage.hpp"

using communication::bolt::Value;

/**
 * TODO (mferencevic): document
 */

constexpr const int SIZE = 131072;
uint8_t data[SIZE];

uint64_t GetBigEndianInt(std::vector<uint8_t> &v, uint8_t len, uint8_t offset = 1) {
  uint64_t ret = 0;
  v.erase(v.begin(), v.begin() + offset);
  for (int i = 0; i < len; ++i) {
    ret <<= 8;
    ret += v[i];
  }
  v.erase(v.begin(), v.begin() + len);
  return ret;
}

void CheckTypeSize(std::vector<uint8_t> &v, int typ, uint64_t size) {
  uint64_t len;
  if ((v[0] & 0xF0) == type_tiny_magic[typ]) {
    len = v[0] & 0x0F;
    v.erase(v.begin(), v.begin() + 1);
  } else if (v[0] == type_8_magic[typ]) {
    len = GetBigEndianInt(v, 1);
  } else if (v[0] == type_16_magic[typ]) {
    len = GetBigEndianInt(v, 2);
  } else if (v[0] == type_32_magic[typ]) {
    len = GetBigEndianInt(v, 4);
  } else {
    FAIL() << "Got wrong marker!";
  }
  ASSERT_EQ(len, size);
}

void CheckInt(std::vector<uint8_t> &output, int64_t value) {
  TestOutputStream output_stream;
  TestBuffer encoder_buffer(output_stream);
  communication::bolt::BaseEncoder<TestBuffer> bolt_encoder(encoder_buffer);
  std::vector<uint8_t> &encoded = output_stream.output;
  bolt_encoder.WriteInt(value);
  CheckOutput(output, encoded.data(), encoded.size(), false);
}

void CheckRecordHeader(std::vector<uint8_t> &v, uint64_t size) {
  CheckOutput(v, (const uint8_t *)"\xB1\x71", 2, false);
  CheckTypeSize(v, LIST, size);
}

TestOutputStream output_stream;
TestBuffer encoder_buffer(output_stream);
communication::bolt::Encoder<TestBuffer> bolt_encoder(encoder_buffer);
std::vector<uint8_t> &output = output_stream.output;

struct BoltEncoder : ::testing::Test {
  // In newer gtest library (1.8.1+) this is changed to SetUpTestSuite
  static void SetUpTestCase() { InitializeData(data, SIZE); }
};

TEST_F(BoltEncoder, NullAndBool) {
  output.clear();
  std::vector<Value> vals;
  vals.push_back(Value());
  vals.push_back(Value(true));
  vals.push_back(Value(false));
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, 3);
  CheckOutput(output, (const uint8_t *)"\xC0\xC3\xC2", 3);
}

TEST_F(BoltEncoder, Int) {
  int N = 28;
  output.clear();
  std::vector<Value> vals;
  for (int i = 0; i < N; ++i) vals.push_back(Value(int_decoded[i]));
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, N);
  for (int i = 0; i < N; ++i) CheckOutput(output, int_encoded[i], int_encoded_len[i], false);
  CheckOutput(output, nullptr, 0);
}

TEST_F(BoltEncoder, Double) {
  int N = 4;
  output.clear();
  std::vector<Value> vals;
  for (int i = 0; i < N; ++i) vals.push_back(Value(double_decoded[i]));
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, N);
  for (int i = 0; i < N; ++i) CheckOutput(output, double_encoded[i], 9, false);
  CheckOutput(output, nullptr, 0);
}

TEST_F(BoltEncoder, String) {
  output.clear();
  std::vector<Value> vals;
  for (uint64_t i = 0; i < sizes_num; ++i) vals.push_back(Value(std::string((const char *)data, sizes[i])));
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, vals.size());
  for (uint64_t i = 0; i < sizes_num; ++i) {
    CheckTypeSize(output, STRING, sizes[i]);
    CheckOutput(output, data, sizes[i], false);
  }
  CheckOutput(output, nullptr, 0);
}

TEST_F(BoltEncoder, List) {
  output.clear();
  std::vector<Value> vals;
  for (uint64_t i = 0; i < sizes_num; ++i) {
    std::vector<Value> val;
    for (uint64_t j = 0; j < sizes[i]; ++j) val.push_back(Value(std::string((const char *)&data[j], 1)));
    vals.push_back(Value(val));
  }
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, vals.size());
  for (uint64_t i = 0; i < sizes_num; ++i) {
    CheckTypeSize(output, LIST, sizes[i]);
    for (uint64_t j = 0; j < sizes[i]; ++j) {
      CheckTypeSize(output, STRING, 1);
      CheckOutput(output, &data[j], 1, false);
    }
  }
  CheckOutput(output, nullptr, 0);
}

TEST_F(BoltEncoder, Map) {
  output.clear();
  std::vector<Value> vals;
  uint8_t buff[10];
  for (int i = 0; i < sizes_num; ++i) {
    std::map<std::string, Value> val;
    for (int j = 0; j < sizes[i]; ++j) {
      sprintf((char *)buff, "%05X", j);
      std::string tmp((char *)buff, 5);
      val.insert(std::make_pair(tmp, Value(tmp)));
    }
    vals.push_back(Value(val));
  }
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, vals.size());
  for (int i = 0; i < sizes_num; ++i) {
    CheckTypeSize(output, MAP, sizes[i]);
    for (int j = 0; j < sizes[i]; ++j) {
      sprintf((char *)buff, "%05X", j);
      CheckTypeSize(output, STRING, 5);
      CheckOutput(output, buff, 5, false);
      CheckTypeSize(output, STRING, 5);
      CheckOutput(output, buff, 5, false);
    }
  }
  CheckOutput(output, nullptr, 0);
}

TEST_F(BoltEncoder, VertexAndEdge) {
  output.clear();

  // create vertex
  storage::Storage db;
  auto dba = db.Access();
  auto va1 = dba.CreateVertex();
  auto va2 = dba.CreateVertex();
  auto l1 = dba.NameToLabel("label1");
  auto l2 = dba.NameToLabel("label2");
  ASSERT_TRUE(va1.AddLabel(l1).HasValue());
  ASSERT_TRUE(va1.AddLabel(l2).HasValue());
  auto p1 = dba.NameToProperty("prop1");
  auto p2 = dba.NameToProperty("prop2");
  storage::PropertyValue pv1(12), pv2(200);
  ASSERT_TRUE(va1.SetProperty(p1, pv1).HasValue());
  ASSERT_TRUE(va1.SetProperty(p2, pv2).HasValue());

  // create edge
  auto et = dba.NameToEdgeType("edgetype");
  auto ea = dba.CreateEdge(&va1, &va2, et);
  auto p3 = dba.NameToProperty("prop3");
  auto p4 = dba.NameToProperty("prop4");
  storage::PropertyValue pv3(42), pv4(1234);
  ASSERT_TRUE(ea->SetProperty(p3, pv3).HasValue());
  ASSERT_TRUE(ea->SetProperty(p4, pv4).HasValue());

  // check everything
  std::vector<Value> vals;
  vals.push_back(*glue::ToBoltValue(query::TypedValue(query::VertexAccessor(va1)), db, storage::View::NEW));
  vals.push_back(*glue::ToBoltValue(query::TypedValue(query::VertexAccessor(va2)), db, storage::View::NEW));
  vals.push_back(*glue::ToBoltValue(query::TypedValue(query::EdgeAccessor(*ea)), db, storage::View::NEW));
  bolt_encoder.MessageRecord(vals);

  // The vertexedge_encoded testdata has hardcoded zeros for IDs,
  // and Memgraph now encodes IDs so we need to check the output
  // part by part.
  CheckOutput(output, vertexedge_encoded, 5, false);
  CheckInt(output, va1.Gid().AsInt());
  CheckOutput(output, vertexedge_encoded + 6, 34, false);
  CheckInt(output, va2.Gid().AsInt());
  CheckOutput(output, vertexedge_encoded + 41, 4, false);
  CheckInt(output, ea->Gid().AsInt());
  CheckInt(output, va1.Gid().AsInt());
  CheckInt(output, va2.Gid().AsInt());
  CheckOutput(output, vertexedge_encoded + 48, 26);
}

TEST_F(BoltEncoder, BoltV1ExampleMessages) {
  // this test checks example messages from: http://boltprotocol.org/v1/

  output.clear();

  // record message
  std::vector<Value> rvals;
  for (int i = 1; i < 4; ++i) rvals.push_back(Value(i));
  bolt_encoder.MessageRecord(rvals);
  CheckOutput(output, (const uint8_t *)"\xB1\x71\x93\x01\x02\x03", 6);

  // success message
  std::string sv1("name"), sv2("age"), sk("fields");
  std::vector<Value> svec;
  svec.push_back(Value(sv1));
  svec.push_back(Value(sv2));
  Value slist(svec);
  std::map<std::string, Value> svals;
  svals.insert(std::make_pair(sk, slist));
  bolt_encoder.MessageSuccess(svals);
  CheckOutput(output,
              (const uint8_t *)"\xB1\x70\xA1\x86\x66\x69\x65\x6C\x64\x73\x92\x84\x6E\x61\x6D\x65\x83\x61\x67\x65", 20);

  // failure message
  std::string fv1("Neo.ClientError.Statement.SyntaxError"), fv2("Invalid syntax.");
  std::string fk1("code"), fk2("message");
  Value ftv1(fv1), ftv2(fv2);
  std::map<std::string, Value> fvals;
  fvals.insert(std::make_pair(fk1, ftv1));
  fvals.insert(std::make_pair(fk2, ftv2));
  bolt_encoder.MessageFailure(fvals);
  CheckOutput(output,
                (const uint8_t *) "\xB1\x7F\xA2\x84\x63\x6F\x64\x65\xD0\x25\x4E\x65\x6F\x2E\x43\x6C\x69\x65\x6E\x74\x45\x72\x72\x6F\x72\x2E\x53\x74\x61\x74\x65\x6D\x65\x6E\x74\x2E\x53\x79\x6E\x74\x61\x78\x45\x72\x72\x6F\x72\x87\x6D\x65\x73\x73\x61\x67\x65\x8F\x49\x6E\x76\x61\x6C\x69\x64\x20\x73\x79\x6E\x74\x61\x78\x2E",
                71);

  // ignored message
  bolt_encoder.MessageIgnored();
  CheckOutput(output, (const uint8_t *)"\xB0\x7E", 2);
}
