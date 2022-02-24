// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>

#include <functional>
#include <sstream>
#include <string_view>

#include "query/procedure/mg_procedure_impl.hpp"

#include "test_utils.hpp"

static void DummyCallback(mgp_list *, mgp_graph *, mgp_result *, mgp_memory *) {}

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
  EXPECT_EQ(mgp_proc_add_arg(proc, "arg1", EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_number)), MGP_ERROR_NO_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER) :: ()");
  EXPECT_EQ(mgp_proc_add_opt_arg(
                proc, "opt1",
                EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_nullable, EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_any)),
                test_utils::CreateValueOwningPtr(EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_null, &memory)).get()),
            MGP_ERROR_NO_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: ()");
  EXPECT_EQ(
      mgp_proc_add_result(
          proc, "res1", EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_list, EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_int))),
      MGP_ERROR_NO_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
  EXPECT_EQ(mgp_proc_add_arg(proc, "arg2", EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_number)), MGP_ERROR_LOGIC_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
  EXPECT_EQ(mgp_proc_add_arg(proc, "arg2", EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_map)), MGP_ERROR_LOGIC_ERROR);
  CheckSignature(proc, "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: (res1 :: LIST OF INTEGER)");
  EXPECT_EQ(mgp_proc_add_deprecated_result(proc, "res2", EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_string)),
            MGP_ERROR_NO_ERROR);
  CheckSignature(proc,
                 "proc(arg1 :: NUMBER, opt1 = Null :: ANY?) :: "
                 "(res1 :: LIST OF INTEGER, DEPRECATED res2 :: STRING)");
  EXPECT_EQ(mgp_proc_add_result(proc, "res2", EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_any)), MGP_ERROR_LOGIC_ERROR);
  EXPECT_EQ(mgp_proc_add_deprecated_result(proc, "res1", EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_any)),
            MGP_ERROR_LOGIC_ERROR);
  EXPECT_EQ(
      mgp_proc_add_opt_arg(proc, "opt2", EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_string),
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
  EXPECT_EQ(mgp_proc_add_opt_arg(
                proc, "opt1",
                EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_nullable, EXPECT_MGP_NO_ERROR(mgp_type *, mgp_type_any)),
                test_utils::CreateValueOwningPtr(EXPECT_MGP_NO_ERROR(mgp_value *, mgp_value_make_null, &memory)).get()),
            MGP_ERROR_NO_ERROR);
  CheckSignature(proc, "proc(opt1 = Null :: ANY?) :: ()");
}

TEST(Module, ReadWriteProcedures) {
  mgp_module module(utils::NewDeleteResource());
  auto *read_proc = EXPECT_MGP_NO_ERROR(mgp_proc *, mgp_module_add_read_procedure, &module, "read", &DummyCallback);
  EXPECT_FALSE(read_proc->info.is_write);
  auto *write_proc = EXPECT_MGP_NO_ERROR(mgp_proc *, mgp_module_add_write_procedure, &module, "write", &DummyCallback);
  EXPECT_TRUE(write_proc->info.is_write);
  mgp_proc read_proc_with_function{"dummy_name",
                                   std::function<void(mgp_list *, mgp_graph *, mgp_result *, mgp_memory *)>{
                                       [](mgp_list *, mgp_graph *, mgp_result *, mgp_memory *) {}},
                                   utils::NewDeleteResource()};
  EXPECT_FALSE(read_proc_with_function.info.is_write);
}
