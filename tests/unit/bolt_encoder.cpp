#include "bolt_common.hpp"
#include "bolt_testdata.hpp"

#include "communication/bolt/v1/encoder/encoder.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/typed_value.hpp"

using query::TypedValue;

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

void CheckRecordHeader(std::vector<uint8_t> &v, uint64_t size) {
  CheckOutput(v, (const uint8_t *)"\xB1\x71", 2, false);
  CheckTypeSize(v, LIST, size);
}

TestSocket socket(10);
TestBuffer encoder_buffer(socket);
communication::bolt::Encoder<TestBuffer> bolt_encoder(encoder_buffer);
std::vector<uint8_t> &output = socket.output;

TEST(BoltEncoder, NullAndBool) {
  std::vector<TypedValue> vals;
  vals.push_back(TypedValue::Null);
  vals.push_back(TypedValue(true));
  vals.push_back(TypedValue(false));
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, 3);
  CheckOutput(output, (const uint8_t *)"\xC0\xC3\xC2", 3);
}

TEST(BoltEncoder, Int) {
  int N = 28;
  std::vector<TypedValue> vals;
  for (int i = 0; i < N; ++i) vals.push_back(TypedValue(int_decoded[i]));
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, N);
  for (int i = 0; i < N; ++i)
    CheckOutput(output, int_encoded[i], int_encoded_len[i], false);
  CheckOutput(output, nullptr, 0);
}

TEST(BoltEncoder, Double) {
  int N = 4;
  std::vector<TypedValue> vals;
  for (int i = 0; i < N; ++i) vals.push_back(TypedValue(double_decoded[i]));
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, N);
  for (int i = 0; i < N; ++i) CheckOutput(output, double_encoded[i], 9, false);
  CheckOutput(output, nullptr, 0);
}

TEST(BoltEncoder, String) {
  std::vector<TypedValue> vals;
  for (uint64_t i = 0; i < sizes_num; ++i)
    vals.push_back(TypedValue(std::string((const char *)data, sizes[i])));
  bolt_encoder.MessageRecord(vals);
  CheckRecordHeader(output, vals.size());
  for (uint64_t i = 0; i < sizes_num; ++i) {
    CheckTypeSize(output, STRING, sizes[i]);
    CheckOutput(output, data, sizes[i], false);
  }
  CheckOutput(output, nullptr, 0);
}

TEST(BoltEncoder, List) {
  std::vector<TypedValue> vals;
  for (uint64_t i = 0; i < sizes_num; ++i) {
    std::vector<TypedValue> val;
    for (uint64_t j = 0; j < sizes[i]; ++j)
      val.push_back(TypedValue(std::string((const char *)&data[j], 1)));
    vals.push_back(TypedValue(val));
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

TEST(BoltEncoder, Map) {
  std::vector<TypedValue> vals;
  uint8_t buff[10];
  for (int i = 0; i < sizes_num; ++i) {
    std::map<std::string, TypedValue> val;
    for (int j = 0; j < sizes[i]; ++j) {
      sprintf((char *)buff, "%05X", j);
      std::string tmp((char *)buff, 5);
      val.insert(std::make_pair(tmp, TypedValue(tmp)));
    }
    vals.push_back(TypedValue(val));
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

TEST(BoltEncoder, VertexAndEdge) {
  // create vertex
  Dbms dbms;
  auto db_accessor = dbms.active();
  auto va1 = db_accessor->insert_vertex();
  auto va2 = db_accessor->insert_vertex();
  std::string l1("label1"), l2("label2");
  va1.add_label(&l1);
  va1.add_label(&l2);
  std::string p1("prop1"), p2("prop2");
  PropertyValue pv1(12), pv2(200);
  va1.PropsSet(&p1, pv1);
  va1.PropsSet(&p2, pv2);

  // create edge
  std::string et("edgetype");
  auto ea = db_accessor->insert_edge(va1, va2, &et);
  std::string p3("prop3"), p4("prop4");
  PropertyValue pv3(42), pv4(1234);
  ea.PropsSet(&p3, pv3);
  ea.PropsSet(&p4, pv4);

  // check everything
  std::vector<TypedValue> vals;
  vals.push_back(TypedValue(va1));
  vals.push_back(TypedValue(va2));
  vals.push_back(TypedValue(ea));
  bolt_encoder.MessageRecord(vals);
  CheckOutput(output, vertexedge_encoded, 74);
}

TEST(BoltEncoder, BoltV1ExampleMessages) {
  // this test checks example messages from: http://boltprotocol.org/v1/

  // record message
  std::vector<TypedValue> rvals;
  for (int i = 1; i < 4; ++i) rvals.push_back(TypedValue(i));
  bolt_encoder.MessageRecord(rvals);
  CheckOutput(output, (const uint8_t *)"\xB1\x71\x93\x01\x02\x03", 6);

  // success message
  std::string sv1("name"), sv2("age"), sk("fields");
  std::vector<TypedValue> svec;
  svec.push_back(TypedValue(sv1));
  svec.push_back(TypedValue(sv2));
  TypedValue slist(svec);
  std::map<std::string, TypedValue> svals;
  svals.insert(std::make_pair(sk, slist));
  bolt_encoder.MessageSuccess(svals);
  CheckOutput(output,
                (const uint8_t *) "\xB1\x70\xA1\x86\x66\x69\x65\x6C\x64\x73\x92\x84\x6E\x61\x6D\x65\x83\x61\x67\x65",
                20);

  // failure message
  std::string fv1("Neo.ClientError.Statement.SyntaxError"),
      fv2("Invalid syntax.");
  std::string fk1("code"), fk2("message");
  TypedValue ftv1(fv1), ftv2(fv2);
  std::map<std::string, TypedValue> fvals;
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

int main(int argc, char **argv) {
  InitializeData(data, SIZE);
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
