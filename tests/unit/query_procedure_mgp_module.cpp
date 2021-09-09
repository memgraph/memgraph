#include <gtest/gtest.h>

#include <sstream>
#include <string_view>

#include "query/procedure/mg_procedure_impl.hpp"

#include "test_utils.hpp"

static void DummyCallback(const mgp_list *, const mgp_graph *, mgp_result *, mgp_memory *) {}

TEST(Module, InvalidProcedureRegistration) {
  mgp_module module(utils::NewDeleteResource());
  mgp_proc *proc{nullptr};
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "dashes-not-supported", DummyCallback, &proc),
            MGP_ERROR_INVALID_ARGUMENT);
  // as u8string this is u8"unicode\u22c6not\u2014supported"
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "unicode\xE2\x8B\x86not\xE2\x80\x94supported", DummyCallback, &proc),
            MGP_ERROR_INVALID_ARGUMENT);
  // as u8string this is u8"`backticks⋆\u22c6won't-save\u2014you`"
  EXPECT_EQ(
      mgp_module_add_read_procedure(&module, "`backticks⋆\xE2\x8B\x86won't-save\xE2\x80\x94you`", DummyCallback, &proc),
      MGP_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "42_name_must_not_start_with_number", DummyCallback, &proc),
            MGP_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "div/", DummyCallback, &proc), MGP_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "mul*", DummyCallback, &proc), MGP_ERROR_INVALID_ARGUMENT);
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "question_mark_is_not_valid?", DummyCallback, &proc),
            MGP_ERROR_INVALID_ARGUMENT);
}

TEST(Module, RegisteringTheSameProcedureMultipleTimes) {
  mgp_module module(utils::NewDeleteResource());
  mgp_proc *proc{nullptr};
  EXPECT_EQ(module.procedures.find("same_name"), module.procedures.end());
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "same_name", DummyCallback, &proc), MGP_ERROR_NO_ERROR);
  EXPECT_NE(module.procedures.find("same_name"), module.procedures.end());
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "same_name", DummyCallback, &proc), MGP_ERROR_LOGIC_ERROR);
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "same_name", DummyCallback, &proc), MGP_ERROR_LOGIC_ERROR);
  EXPECT_NE(module.procedures.find("same_name"), module.procedures.end());
}

TEST(Module, CaseSensitiveProcedureNames) {
  mgp_module module(utils::NewDeleteResource());
  EXPECT_TRUE(module.procedures.empty());
  mgp_proc *proc{nullptr};
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "not_same", DummyCallback, &proc), MGP_ERROR_NO_ERROR);
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "NoT_saME", DummyCallback, &proc), MGP_ERROR_NO_ERROR);
  EXPECT_EQ(mgp_module_add_read_procedure(&module, "NOT_SAME", DummyCallback, &proc), MGP_ERROR_NO_ERROR);
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
  auto *proc = EXPECT_MGP_NO_ERROR(mgp_proc *, mgp_module_add_read_procedure, &module, "proc", &DummyCallback);
  CheckSignature(proc, "proc() :: ()");
  EXPECT_EQ(mgp_proc_add_arg(proc, "arg1", EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number)), MGP_ERROR_NO_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER) :: ()");
  EXPECT_EQ(
      mgp_proc_add_opt_arg(
          proc, "opt1",
          EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any)),
          test_utils::CreateValueOwningPtr(EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_null, &memory)).get()),
      MGP_ERROR_NO_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: ()");
  EXPECT_EQ(mgp_proc_add_result(proc, "res1",
                                EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_list,
                                                    EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_int))),
            MGP_ERROR_NO_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
  EXPECT_EQ(mgp_proc_add_arg(proc, "arg2", EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_number)),
            MGP_ERROR_LOGIC_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
  EXPECT_EQ(mgp_proc_add_arg(proc, "arg2", EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_map)), MGP_ERROR_LOGIC_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
  EXPECT_EQ(mgp_proc_add_deprecated_result(proc, "res2", EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string)),
            MGP_ERROR_NO_ERROR);
  CheckSignature(proc,
                 "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: "
                 "(res1 :: LIST OF INTEGER, DEPRECATED res2 :: STRING)");
  EXPECT_EQ(mgp_proc_add_result(proc, "res2", EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any)),
            MGP_ERROR_LOGIC_ERROR);
  EXPECT_EQ(mgp_proc_add_deprecated_result(proc, "res1", EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any)),
            MGP_ERROR_LOGIC_ERROR);
  EXPECT_EQ(
      mgp_proc_add_opt_arg(proc, "opt2", EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_string),
                           test_utils::CreateValueOwningPtr(
                               EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_string, "string=\"value\"", &memory))
                               .get()),
      MGP_ERROR_NO_ERROR);
  CheckSignature(proc,
                 "proc(arg1 :: NUMBER, opt1 = Null :: ANY?, "
                 "opt2 = \"string=\\\"value\\\"\" :: STRING) :: "
                 "(res1 :: LIST OF INTEGER, DEPRECATED res2 :: STRING)");
}

TEST(Module, ProcedureSignatureOnlyOptArg) {
  mgp_memory memory{utils::NewDeleteResource()};
  mgp_module module(utils::NewDeleteResource());
  auto *proc = EXPECT_MGP_NO_ERROR(mgp_proc *, mgp_module_add_read_procedure, &module, "proc", &DummyCallback);
  EXPECT_EQ(
      mgp_proc_add_opt_arg(
          proc, "opt1",
          EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_nullable, EXPECT_MGP_NO_ERROR(const mgp_type *, mgp_type_any)),
          test_utils::CreateValueOwningPtr(EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_null, &memory)).get()),
      MGP_ERROR_NO_ERROR);
  CheckSignature(proc, "proc(opt1 = Null :: ANY?) :: ()");
}
