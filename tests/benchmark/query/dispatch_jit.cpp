// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// What compiling an expression to native code costs and what it buys. The
// expression matches the one in dispatch.cpp, over unboxed integers.
//
// Execution speed is the easy half and lands where hand-written native code
// lands. The half that decides whether this can pay is compile latency, which
// is charged once per query against a saving charged once per row, so the two
// together give the row count a query must reach before it is ahead.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/LLVMContext.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/Error.h"
#include "llvm/Support/TargetSelect.h"

namespace {

using ExprFn = bool (*)(const int64_t *);

struct Jitted {
  std::unique_ptr<llvm::orc::LLJIT> jit;
  ExprFn fn{nullptr};
};

// Builds (s0 + s1) == s2 && s0 == s3 and hands back the native entry point.
Jitted Compile() {
  auto ctx = std::make_unique<llvm::LLVMContext>();
  auto mod = std::make_unique<llvm::Module>("expr", *ctx);
  llvm::IRBuilder<> b(*ctx);

  auto *i64 = llvm::Type::getInt64Ty(*ctx);
  auto *i1 = llvm::Type::getInt1Ty(*ctx);
  auto *ptr = llvm::PointerType::getUnqual(*ctx);

  auto *ft = llvm::FunctionType::get(i1, {ptr}, false);
  auto *f = llvm::Function::Create(ft, llvm::Function::ExternalLinkage, "expr", mod.get());
  b.SetInsertPoint(llvm::BasicBlock::Create(*ctx, "entry", f));

  auto *slots = f->getArg(0);
  auto load = [&](int64_t i) -> llvm::Value * {
    return b.CreateLoad(i64, b.CreateConstInBoundsGEP1_64(i64, slots, i));
  };

  auto *s0 = load(0);
  auto *s1 = load(1);
  auto *s2 = load(2);
  auto *s3 = load(3);
  auto *lhs = b.CreateICmpEQ(b.CreateAdd(s0, s1), s2);
  auto *rhs = b.CreateICmpEQ(s0, s3);
  b.CreateRet(b.CreateAnd(lhs, rhs));

  auto jit = llvm::cantFail(llvm::orc::LLJITBuilder().create());
  llvm::cantFail(jit->addIRModule(llvm::orc::ThreadSafeModule(std::move(mod), std::move(ctx))));
  auto sym = llvm::cantFail(jit->lookup("expr"));
  return Jitted{std::move(jit), sym.toPtr<ExprFn>()};
}

std::vector<std::vector<int64_t>> MakeSlots() {
  return {{3, 4, 7, 3}, {1, 2, 9, 1}, {5, 5, 10, 5}, {2, 2, 4, 8}, {6, 1, 7, 6}};
}

struct InitTargets {
  InitTargets() {
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
  }
};

}  // namespace

// Compiling one expression: paid once per query.
static void Jit_CompileLatency(benchmark::State &state) {
  static InitTargets init;
  for (auto _ : state) {
    auto j = Compile();
    benchmark::DoNotOptimize(j.fn);
  }
  state.SetItemsProcessed(state.iterations());
}

// Running it: paid once per row.
static void Jit_Execute(benchmark::State &state) {
  static InitTargets init;
  auto j = Compile();
  auto slots = MakeSlots();
  if (j.fn == nullptr) {
    state.SkipWithError("jit produced no entry point");
    return;
  }
  // The jitted answer must match what the expression means.
  for (const auto &s : slots) {
    const bool want = (s[0] + s[1]) == s[2] && s[0] == s[3];
    if (j.fn(s.data()) != want) {
      state.SkipWithError("jitted code disagrees with the expression");
      return;
    }
  }
  size_t i = 0;
  for (auto _ : state) {
    const auto &s = slots[i];
    if (++i == slots.size()) i = 0;
    bool r = j.fn(s.data());
    benchmark::DoNotOptimize(r);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK(Jit_CompileLatency)->Unit(benchmark::kMicrosecond);
BENCHMARK(Jit_Execute);

BENCHMARK_MAIN();
