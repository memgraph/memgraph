#pragma once

#include "query/i_plan_cpu.hpp"
#include "query/strip/stripped.hpp"

/*
 * Query Program Contains:
 *     * Query Plan
 *     * Query Arguments (Stripped)
 */
template <typename Stream>
struct QueryProgram
{
    using plan_t = IPlanCPU<Stream>;

    QueryProgram(plan_t *plan, QueryStripped &&stripped)
        : plan(plan), stripped(std::forward<QueryStripped>(stripped))
    {
    }

    QueryProgram(QueryProgram &other) = delete;
    QueryProgram(QueryProgram &&other) = default;

    plan_t *plan;
    QueryStripped stripped;
};
