#include "bolt_common.hpp"

#include "communication/bolt/v1/encoder/encoder.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "query/backend/cpp/typed_value.hpp"


class TestBuffer {
 public:
  TestBuffer(TestSocket& socket) : socket_(socket) {}

  void Write(const uint8_t* data, size_t n) {
    socket_.Write(data, n);
  }

  void Flush() {}

 private:
  TestSocket& socket_;
};


const int64_t int_input[] = { 0, -1, -8, -16, 1, 63, 127, -128, -20, -17, -32768, -12345, -129, 128, 12345, 32767, -2147483648L, -12345678L, -32769L, 32768L, 12345678L, 2147483647L, -9223372036854775807L, -12345678912345L, -2147483649L, 2147483648L, 12345678912345L, 9223372036854775807 };
const uint8_t int_output[][10] = { "\x00", "\xFF", "\xF8", "\xF0", "\x01", "\x3F", "\x7F", "\xC8\x80", "\xC8\xEC", "\xC8\xEF", "\xC9\x80\x00", "\xC9\xCF\xC7", "\xC9\xFF\x7F", "\xC9\x00\x80", "\xC9\x30\x39", "\xC9\x7F\xFF", "\xCA\x80\x00\x00\x00", "\xCA\xFF\x43\x9E\xB2", "\xCA\xFF\xFF\x7F\xFF", "\xCA\x00\x00\x80\x00", "\xCA\x00\xBC\x61\x4E", "\xCA\x7F\xFF\xFF\xFF", "\xCB\x80\x00\x00\x00\x00\x00\x00\x01", "\xCB\xFF\xFF\xF4\xC5\x8C\x31\xA4\xA7", "\xCB\xFF\xFF\xFF\xFF\x7F\xFF\xFF\xFF", "\xCB\x00\x00\x00\x00\x80\x00\x00\x00", "\xCB\x00\x00\x0B\x3A\x73\xCE\x5B\x59", "\xCB\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF" };
const uint32_t int_output_len[] = { 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3, 5, 5, 5, 5, 5, 5, 9, 9, 9, 9, 9, 9 };


const double double_input[] = { 5.834, 108.199, 43677.9882, 254524.5851 };
const uint8_t double_output[][10] = { "\xC1\x40\x17\x56\x04\x18\x93\x74\xBC", "\xC1\x40\x5B\x0C\xBC\x6A\x7E\xF9\xDB", "\xC1\x40\xE5\x53\xBF\x9F\x55\x9B\x3D", "\xC1\x41\x0F\x11\xE4\xAE\x48\xE8\xA7"};


const uint8_t vertexedge_output[] = "\xB1\x71\x93\xB3\x4E\x00\x92\x86\x6C\x61\x62\x65\x6C\x31\x86\x6C\x61\x62\x65\x6C\x32\xA2\x85\x70\x72\x6F\x70\x31\x0C\x85\x70\x72\x6F\x70\x32\xC9\x00\xC8\xB3\x4E\x00\x90\xA0\xB5\x52\x00\x00\x00\x88\x65\x64\x67\x65\x74\x79\x70\x65\xA2\x85\x70\x72\x6F\x70\x33\x2A\x85\x70\x72\x6F\x70\x34\xC9\x04\xD2";


constexpr const int SIZE = 131072;
uint8_t data[SIZE];
const uint64_t sizes[] = { 0, 1, 5, 15, 16, 120, 255, 256, 12345, 65535, 65536, 100000 };
const uint64_t sizes_num = 12;


constexpr const int STRING = 0, LIST = 1, MAP = 2;
const uint8_t type_tiny_magic[] = { 0x80, 0x90, 0xA0 };
const uint8_t type_8_magic[] = { 0xD0, 0xD4, 0xD8 };
const uint8_t type_16_magic[] = { 0xD1, 0xD5, 0xD9 };
const uint8_t type_32_magic[] = { 0xD2, 0xD6, 0xDA };

void check_type_size(std::vector<uint8_t>& v, int typ, uint64_t size) {
  if (size <= 15) {
    uint8_t len = size;
    len &= 0x0F;
    len += type_tiny_magic[typ];
    check_output(v, &len, 1, false);
  } else if (size <= 255) {
    uint8_t len = size;
    check_output(v, &type_8_magic[typ], 1, false);
    check_output(v, &len, 1, false);
  } else if (size <= 65536) {
    uint16_t len = size;
    len = bswap(len);
    check_output(v, &type_16_magic[typ], 1, false);
    check_output(v, reinterpret_cast<const uint8_t*> (&len), 2, false);
  } else {
    uint32_t len = size;
    len = bswap(len);
    check_output(v, &type_32_magic[typ], 1, false);
    check_output(v, reinterpret_cast<const uint8_t*> (&len), 4, false);
  }
}

void check_record_header(std::vector<uint8_t>& v, uint64_t size) {
  check_output(v, (const uint8_t*) "\xB1\x71", 2, false);
  check_type_size(v, LIST, size);
}


TestSocket socket(10);
communication::bolt::Encoder<TestBuffer, TestSocket> bolt_encoder(socket);
std::vector<uint8_t>& output = socket.output;


TEST(BoltEncoder, NullAndBool) {
  std::vector<TypedValue> vals;
  vals.push_back(TypedValue::Null);
  vals.push_back(TypedValue(true));
  vals.push_back(TypedValue(false));
  bolt_encoder.MessageRecord(vals);
  check_record_header(output, 3);
  check_output(output, (const uint8_t*) "\xC0\xC3\xC2", 3);
}

TEST(BoltEncoder, Int) {
  int N = 28;
  std::vector<TypedValue> vals;
  for (int i = 0; i < N; ++i)
    vals.push_back(TypedValue(int_input[i]));
  bolt_encoder.MessageRecord(vals);
  check_record_header(output, N);
  for (int i = 0; i < N; ++i)
    check_output(output, int_output[i], int_output_len[i], false);
  check_output(output, nullptr, 0);
}

TEST(BoltEncoder, Double) {
  int N = 4;
  std::vector<TypedValue> vals;
  for (int i = 0; i < N; ++i)
    vals.push_back(TypedValue(double_input[i]));
  bolt_encoder.MessageRecord(vals);
  check_record_header(output, N);
  for (int i = 0; i < N; ++i)
    check_output(output, double_output[i], 9, false);
  check_output(output, nullptr, 0);
}

TEST(BoltEncoder, String) {
  std::vector<TypedValue> vals;
  for (int i = 0; i < sizes_num; ++i)
    vals.push_back(TypedValue(std::string((const char*) data, sizes[i])));
  bolt_encoder.MessageRecord(vals);
  check_record_header(output, vals.size());
  for (int i = 0; i < sizes_num; ++i) {
    check_type_size(output, STRING, sizes[i]);
    check_output(output, data, sizes[i], false);
  }
  check_output(output, nullptr, 0);
}

TEST(BoltEncoder, List) {
  std::vector<TypedValue> vals;
  for (int i = 0; i < sizes_num; ++i) {
    std::vector<TypedValue> val;
    for (int j = 0; j < sizes[i]; ++j)
      val.push_back(TypedValue(std::string((const char*) &data[j], 1)));
    vals.push_back(TypedValue(val));
  }
  bolt_encoder.MessageRecord(vals);
  check_record_header(output, vals.size());
  for (int i = 0; i < sizes_num; ++i) {
    check_type_size(output, LIST, sizes[i]);
    for (int j = 0; j < sizes[i]; ++j) {
      check_type_size(output, STRING, 1);
      check_output(output, &data[j], 1, false);
    }
  }
  check_output(output, nullptr, 0);
}

TEST(BoltEncoder, Map) {
  std::vector<TypedValue> vals;
  uint8_t buff[10];
  for (int i = 0; i < sizes_num; ++i) {
    std::map<std::string, TypedValue> val;
    for (int j = 0; j < sizes[i]; ++j) {
      sprintf((char*) buff, "%05X", j);
      std::string tmp((char*) buff, 5);
      val.insert(std::make_pair(tmp, TypedValue(tmp)));
    }
    vals.push_back(TypedValue(val));
  }
  bolt_encoder.MessageRecord(vals);
  check_record_header(output, vals.size());
  for (int i = 0; i < sizes_num; ++i) {
    check_type_size(output, MAP, sizes[i]);
    for (int j = 0; j < sizes[i]; ++j) {
      sprintf((char*) buff, "%05X", j);
      check_type_size(output, STRING, 5);
      check_output(output, buff, 5, false);
      check_type_size(output, STRING, 5);
      check_output(output, buff, 5, false);
    }
  }
  check_output(output, nullptr, 0);
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
  check_output(output, vertexedge_output, 74);
}

TEST(BoltEncoder, BoltV1ExampleMessages) {
  // this test checks example messages from: http://boltprotocol.org/v1/

  // record message
  std::vector<TypedValue> rvals;
  for (int i = 1; i < 4; ++i) rvals.push_back(TypedValue(i));
  bolt_encoder.MessageRecord(rvals);
  check_output(output, (const uint8_t*) "\xB1\x71\x93\x01\x02\x03", 6);

  // success message
  std::string sv1("name"), sv2("age"), sk("fields");
  std::vector<TypedValue> svec;
  svec.push_back(TypedValue(sv1));
  svec.push_back(TypedValue(sv2));
  TypedValue slist(svec);
  std::map<std::string, TypedValue> svals;
  svals.insert(std::make_pair(sk, slist));
  bolt_encoder.MessageSuccess(svals);
  check_output(output, (const uint8_t*) "\xB1\x70\xA1\x86\x66\x69\x65\x6C\x64\x73\x92\x84\x6E\x61\x6D\x65\x83\x61\x67\x65", 20);

  // failure message
  std::string fv1("Neo.ClientError.Statement.SyntaxError"), fv2("Invalid syntax.");
  std::string fk1("code"), fk2("message");
  TypedValue ftv1(fv1), ftv2(fv2);
  std::map<std::string, TypedValue> fvals;
  fvals.insert(std::make_pair(fk1, ftv1));
  fvals.insert(std::make_pair(fk2, ftv2));
  bolt_encoder.MessageFailure(fvals);
  check_output(output, (const uint8_t*) "\xB1\x7F\xA2\x84\x63\x6F\x64\x65\xD0\x25\x4E\x65\x6F\x2E\x43\x6C\x69\x65\x6E\x74\x45\x72\x72\x6F\x72\x2E\x53\x74\x61\x74\x65\x6D\x65\x6E\x74\x2E\x53\x79\x6E\x74\x61\x78\x45\x72\x72\x6F\x72\x87\x6D\x65\x73\x73\x61\x67\x65\x8F\x49\x6E\x76\x61\x6C\x69\x64\x20\x73\x79\x6E\x74\x61\x78\x2E", 71);

  // ignored message
  bolt_encoder.MessageIgnored();
  check_output(output, (const uint8_t*) "\xB0\x7E", 2);
}


int main(int argc, char** argv) {
  initialize_data(data, SIZE);
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
