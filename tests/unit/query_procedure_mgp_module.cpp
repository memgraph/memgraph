#include <gtest/gtest.h>

#include <sstream>
#include <string_view>

#include "query/procedure/mg_procedure_impl.hpp"

static void DummyCallback(const mgp_list *, const mgp_graph *, mgp_result *,
                          mgp_memory *) {}

TEST(Module, InvalidProcedureRegistration) {
  mgp_module module(utils::NewDeleteResource());
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "dashes-not-supported",
                                             DummyCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(
      &module, u8"unicode\u22c6not\u2014supported", DummyCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(
      &module, u8"`backticks⋆\u22c6won't-save\u2014you`", DummyCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(
      &module, "42_name_must_not_start_with_number", DummyCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "div/", DummyCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(&module, "mul*", DummyCallback));
  EXPECT_FALSE(mgp_module_add_read_procedure(
      &module, "question_mark_is_not_valid?", DummyCallback));
}

TEST(Module, RegisteringTheSameProcedureMultipleTimes) {
  mgp_module module(utils::NewDeleteResource());
  EXPECT_EQ(module.procedures.find("same_name"), module.procedures.end());
  EXPECT_TRUE(
      mgp_module_add_read_procedure(&module, "same_name", DummyCallback));
  EXPECT_NE(module.procedures.find("same_name"), module.procedures.end());
  EXPECT_FALSE(
      mgp_module_add_read_procedure(&module, "same_name", DummyCallback));
  EXPECT_FALSE(
      mgp_module_add_read_procedure(&module, "same_name", DummyCallback));
  EXPECT_NE(module.procedures.find("same_name"), module.procedures.end());
}

TEST(Module, CaseSensitiveProcedureNames) {
  mgp_module module(utils::NewDeleteResource());
  EXPECT_TRUE(module.procedures.empty());
  EXPECT_TRUE(
      mgp_module_add_read_procedure(&module, "not_same", DummyCallback));
  EXPECT_TRUE(
      mgp_module_add_read_procedure(&module, "NoT_saME", DummyCallback));
  EXPECT_TRUE(
      mgp_module_add_read_procedure(&module, "NOT_SAME", DummyCallback));
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
  auto *proc = mgp_module_add_read_procedure(&module, "proc", DummyCallback);
  CheckSignature(proc, "proc() :: ()");
  mgp_proc_add_arg(proc, "arg1", mgp_type_number());
  CheckSignature(proc, "proc(arg1 :: NUMBER) :: ()");
  mgp_proc_add_opt_arg(proc, "opt1", mgp_type_nullable(mgp_type_any()),
                       mgp_value_make_null(&memory));
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: ()");
  mgp_proc_add_result(proc, "res1", mgp_type_list(mgp_type_int()));
  CheckSignature(
      proc,
      "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
  EXPECT_FALSE(mgp_proc_add_arg(proc, "arg2", mgp_type_number()));
  CheckSignature(
      proc,
      "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
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
                       mgp_value_make_string("string=\"value\"", &memory));
  CheckSignature(proc,
                 "proc(arg1 :: NUMBER, opt1 = Null :: ANY?, "
                 "opt2 = \"string=\\\"value\\\"\" :: STRING) :: "
                 "(res1 :: LIST OF INTEGER, DEPRECATED res2 :: STRING)");
}

TEST(Module, ProcedureSignatureOnlyOptArg) {
  mgp_memory memory{utils::NewDeleteResource()};
  mgp_module module(utils::NewDeleteResource());
  auto *proc = mgp_module_add_read_procedure(&module, "proc", DummyCallback);
  mgp_proc_add_opt_arg(proc, "opt1", mgp_type_nullable(mgp_type_any()),
                       mgp_value_make_null(&memory));
  CheckSignature(proc, "proc(opt1 = Null :: ANY?) :: ()");
}
