#include <gtest/gtest.h>

#include <functional>
#include <sstream>
#include <string_view>

#include "query/procedure/mg_procedure_impl.hpp"

#include "test_utils.hpp"

static void DummyReadCallback(const mgp_list *, const mgp_graph *, mgp_result *, mgp_memory *) {}
static void DummyWriteCallback(const mgp_list *, mgp_graph *, mgp_result *, mgp_memory *) {}

TEST(Module, InvalidProcedureRegistration) {
  mgp_module module(utils::NewDeleteResource());
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "dashes-not-supported", DummyReadCallback));
  // as u8string this is u8"unicode\u22c6not\u2014supported"
  EXPECT_FALSE(
      mgp_module_add_read_procedure(&module, "unicode\xE2\x8B\x86not\xE2\x80\x94supported", DummyReadCallback));
  // as u8string this is u8"`backticks⋆\u22c6won't-save\u2014you`"
  EXPECT_FALSE(
      mgp_module_add_read_procedure(&module, "`backticks⋆\xE2\x8B\x86won't-save\xE2\x80\x94you`", DummyReadCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "42_name_must_not_start_with_number", DummyReadCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "div/", DummyReadCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "mul*", DummyReadCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "question_mark_is_not_valid?", DummyReadCallback));
}

TEST(Module, RegisteringTheSameProcedureMultipleTimes) {
  mgp_module module(utils::NewDeleteResource());
  EXPECT_EQ(module.procedures.find("same_name"), module.procedures.end());
  EXPECT_TRUE(mgp_module_add_read_procedure(&module, "same_name", DummyReadCallback));
  EXPECT_NE(module.procedures.find("same_name"), module.procedures.end());
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "same_name", DummyReadCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "same_name", DummyReadCallback));
  EXPECT_NE(module.procedures.find("same_name"), module.procedures.end());
}

TEST(Module, CaseSensitiveProcedureNames) {
  mgp_module module(utils::NewDeleteResource());
  EXPECT_TRUE(module.procedures.empty());
  EXPECT_TRUE(mgp_module_add_read_procedure(&module, "not_same", DummyReadCallback));
  EXPECT_TRUE(mgp_module_add_read_procedure(&module, "NoT_saME", DummyReadCallback));
  EXPECT_TRUE(mgp_module_add_read_procedure(&module, "NOT_SAME", DummyReadCallback));
  EXPECT_EQ(module.procedures.size(), 3U);
}

static void CheckSignature(const mgp_proc *proc, const std::string &expected) {
  std::stringstream ss;
  query::procedure::PrintProcSignature(*proc, &ss);
  EXPECT_EQ(ss.str(), expected);
}

TEST(Module, ProcedureSignature) {
  mgp_memory memory{utils::NewDeleteResource()};
  mgp_module module(utils::NewDeleteResource());
  auto *proc = mgp_module_add_read_procedure(&module, "proc", DummyReadCallback);
  CheckSignature(proc, "proc() :: ()");
  mgp_proc_add_arg(proc, "arg1", mgp_type_number());
  CheckSignature(proc, "proc(arg1 :: NUMBER) :: ()");
  mgp_proc_add_opt_arg(proc, "opt1", mgp_type_nullable(mgp_type_any()),
                       test_utils::CreateValueOwningPtr(mgp_value_make_null(&memory)).get());
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: ()");
  mgp_proc_add_result(proc, "res1", mgp_type_list(mgp_type_int()));
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
  EXPECT_FALSE(mgp_proc_add_arg(proc, "arg2", mgp_type_number()));
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
  EXPECT_FALSE(mgp_proc_add_arg(proc, "arg2", mgp_type_map()));
  CheckSignature(proc,
                 "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: "
                 "(res1 :: LIST OF INTEGER)");
  mgp_proc_add_deprecated_result(proc, "res2", mgp_type_string());
  CheckSignature(proc,
                 "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: "
                 "(res1 :: LIST OF INTEGER, DEPRECATED res2 :: STRING)");
  EXPECT_FALSE(mgp_proc_add_result(proc, "res2", mgp_type_any()));
  EXPECT_FALSE(mgp_proc_add_deprecated_result(proc, "res1", mgp_type_any()));
  mgp_proc_add_opt_arg(proc, "opt2", mgp_type_string(),
                       test_utils::CreateValueOwningPtr(mgp_value_make_string("string=\"value\"", &memory)).get());
  CheckSignature(proc,
                 "proc(arg1 :: NUMBER, opt1 = Null :: ANY?, "
                 "opt2 = \"string=\\\"value\\\"\" :: STRING) :: "
                 "(res1 :: LIST OF INTEGER, DEPRECATED res2 :: STRING)");
}

TEST(Module, ProcedureSignatureOnlyOptArg) {
  mgp_memory memory{utils::NewDeleteResource()};
  mgp_module module(utils::NewDeleteResource());
  auto *proc = mgp_module_add_read_procedure(&module, "proc", DummyReadCallback);
  mgp_proc_add_opt_arg(proc, "opt1", mgp_type_nullable(mgp_type_any()),
                       test_utils::CreateValueOwningPtr(mgp_value_make_null(&memory)).get());
  CheckSignature(proc, "proc(opt1 = Null :: ANY?) :: ()");
}

TEST(Module, ReadWriteProcedures) {
  mgp_module module(utils::NewDeleteResource());
  auto *read_proc = mgp_module_add_read_procedure(&module, "read", DummyReadCallback);
  EXPECT_FALSE(read_proc->is_write_procedure);
  auto *write_proc = mgp_module_add_write_procedure(&module, "write", DummyWriteCallback);
  EXPECT_TRUE(write_proc->is_write_procedure);
  mgp_proc read_proc_with_function{"dummy_name",
                                   std::function<void(const mgp_list *, const mgp_graph *, mgp_result *, mgp_memory *)>{
                                       [](const mgp_list *, const mgp_graph *, mgp_result *, mgp_memory *) {}},
                                   utils::NewDeleteResource()};
  EXPECT_FALSE(read_proc_with_function.is_write_procedure);
}
