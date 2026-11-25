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

// C ABI of the Rust reference model in src/lib.rs. Keep the two in step.
//
// Terms are numbered from 0 in the order they are added. A block label is
// the smallest term index in its block. Every query except add, merge and
// num_terms answers from the closure computed by the last egraph_oracle_close.

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define EGRAPH_ORACLE_NONE UINT32_MAX

typedef struct egraph_oracle egraph_oracle;

egraph_oracle *egraph_oracle_new(void);
void egraph_oracle_free(egraph_oracle *model);

uint32_t egraph_oracle_add(egraph_oracle *model, uint8_t symbol, uint64_t disambiguator, uint32_t const *children,
                           size_t len);
void egraph_oracle_merge(egraph_oracle *model, uint32_t first, uint32_t second);
void egraph_oracle_close(egraph_oracle *model);
uint32_t egraph_oracle_num_terms(egraph_oracle *model);

uint32_t egraph_oracle_block_of(egraph_oracle *model, uint32_t term);
bool egraph_oracle_equivalent(egraph_oracle *model, uint32_t first, uint32_t second);
uint32_t egraph_oracle_num_blocks(egraph_oracle *model);
uint32_t egraph_oracle_num_enodes(egraph_oracle *model);
uint32_t egraph_oracle_enodes_in_block(egraph_oracle *model, uint32_t block);
uint32_t egraph_oracle_find_enode(egraph_oracle *model, uint8_t symbol, uint64_t disambiguator,
                                  uint32_t const *child_blocks, size_t len);

#ifdef __cplusplus
}
#endif
