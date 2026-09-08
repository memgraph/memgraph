# Memory-aware build parallelism.
#
# The heaviest translation units in this tree, and the links of the biggest
# binaries, each need several gigabytes of RSS. `-j$(nproc)` therefore asks for
# far more memory than a typical laptop or CI container has, and the build dies
# in the OOM killer rather than reporting a compiler error. Ninja job pools cap
# how many compile and link steps run at once regardless of the `-j` the caller
# passed, so every entry point (build.sh, mgbuild.sh, a bare `cmake --build`)
# inherits the cap without having to compute one.
#
# Overrides, all at configure time:
#   -DMG_COMPILE_JOBS=N / -DMG_LINK_JOBS=N   pin the pool sizes explicitly
#   -DMG_RESERVE_CORES=N                     leave N cores free for other work
#   -DMG_MEMORY_PER_COMPILE_JOB_MB=N         retune the per-job budgets
#   -DMG_MEMORY_PER_LINK_JOB_MB=N
#   -DMG_LIMIT_PARALLELISM_BY_MEMORY=OFF     no pools, `-j` alone decides

option(MG_LIMIT_PARALLELISM_BY_MEMORY
       "Cap concurrent compile and link steps to what the machine's memory allows" ON)

# Sized to the most expensive steps in the tree rather than the average, so a
# full pool cannot exceed the budget even when every slot lands on a heavy one.
set(MG_MEMORY_PER_COMPILE_JOB_MB 4096 CACHE STRING
    "Memory budgeted for one compile step, in MiB")
set(MG_MEMORY_PER_LINK_JOB_MB 6144 CACHE STRING
    "Memory budgeted for one link step, in MiB")
set(MG_RESERVE_CORES 0 CACHE STRING
    "Cores left free for other work, withheld from every pool")
set(MG_COMPILE_JOBS 0 CACHE STRING
    "Concurrent compile steps; 0 derives it from available memory")
set(MG_LINK_JOBS 0 CACHE STRING
    "Concurrent link steps; 0 derives it from available memory")

# Memory this build must not claim, so the machine stays usable and the kernel
# keeps a page cache. Bounded at both ends: a bare fraction reserves nothing
# worth having on a small machine, and everything worth having on a big one.
set(MG_MEMORY_RESERVE_FRACTION 10)
set(MG_MEMORY_RESERVE_MIN_MB 1024)
set(MG_MEMORY_RESERVE_MAX_MB 4096)

# Every memory.max / memory.high / memory.limit_in_bytes that applies to this
# process, as a list of file paths. A limit set on an ancestor cgroup binds
# just as hard as one set on the leaf, and the leaf is only visible at the
# filesystem root when the process is in its own cgroup namespace, which is
# true of a container and false of a systemd scope.
function(_mg_cgroup_limit_files out_var)
    set(files "")
    if(NOT EXISTS /proc/self/cgroup)
        set(${out_var} "" PARENT_SCOPE)
        return()
    endif()
    file(READ /proc/self/cgroup cgroup_lines)

    # v2: a single unified hierarchy, written as "0::<path>".
    if(cgroup_lines MATCHES "(^|\n)0::([^\n]*)")
        _mg_cgroup_chain(v2_files /sys/fs/cgroup "${CMAKE_MATCH_2}" memory.max memory.high)
        list(APPEND files ${v2_files})
    endif()

    # v1: one hierarchy per controller, written as "<id>:memory:<path>".
    if(cgroup_lines MATCHES "(^|\n)[0-9]+:memory:([^\n]*)")
        _mg_cgroup_chain(v1_files /sys/fs/cgroup/memory "${CMAKE_MATCH_2}" memory.limit_in_bytes)
        list(APPEND files ${v1_files})
    endif()

    set(${out_var} "${files}" PARENT_SCOPE)
endfunction()

# The named limit files in <mount><path> and in each of its ancestors up to
# <mount>.
function(_mg_cgroup_chain out_var mount path)
    set(files "")
    string(REGEX REPLACE "/+$" "" dir "${mount}${path}")
    while(dir MATCHES "^${mount}")
        foreach(name IN LISTS ARGN)
            if(EXISTS "${dir}/${name}")
                list(APPEND files "${dir}/${name}")
            endif()
        endforeach()
        if(dir STREQUAL "${mount}")
            set(dir "")
        else()
            get_filename_component(parent "${dir}" DIRECTORY)
            if(parent STREQUAL "${dir}")
                set(dir "")
            else()
                set(dir "${parent}")
            endif()
        endif()
    endwhile()
    set(${out_var} "${files}" PARENT_SCOPE)
endfunction()

# The memory this build may use, in MiB. Under a cgroup limit the host's RAM is
# not what the build gets: the limit is, and exceeding it is what triggers the
# OOM kill.
function(_mg_memory_budget_mb out_var)
    cmake_host_system_information(RESULT budget QUERY TOTAL_PHYSICAL_MEMORY)

    _mg_cgroup_limit_files(limit_files)
    foreach(limit_file IN LISTS limit_files)
        file(READ "${limit_file}" limit)
        string(STRIP "${limit}" limit)
        # "max" means unlimited; cgroup v1 spells that as a sentinel near the
        # top of the 64-bit range, which the MiB conversion shrinks to
        # something merely astronomical rather than something meaningful.
        if(limit MATCHES "^[0-9]+$")
            math(EXPR limit_mb "${limit} / 1048576")
            if(limit_mb GREATER 0 AND limit_mb LESS 1048576 AND limit_mb LESS budget)
                set(budget ${limit_mb})
            endif()
        endif()
    endforeach()

    math(EXPR reserve "${budget} * ${MG_MEMORY_RESERVE_FRACTION} / 100")
    if(reserve LESS ${MG_MEMORY_RESERVE_MIN_MB})
        set(reserve ${MG_MEMORY_RESERVE_MIN_MB})
    elseif(reserve GREATER ${MG_MEMORY_RESERVE_MAX_MB})
        set(reserve ${MG_MEMORY_RESERVE_MAX_MB})
    endif()
    math(EXPR budget "${budget} - ${reserve}")
    if(budget LESS 1024)
        set(budget 1024)
    endif()
    set(${out_var} ${budget} PARENT_SCOPE)
endfunction()

function(_mg_jobs_for_budget out_var budget_mb per_job_mb core_count)
    if(per_job_mb LESS_EQUAL 0)
        set(${out_var} ${core_count} PARENT_SCOPE)
        return()
    endif()
    math(EXPR jobs "${budget_mb} / ${per_job_mb}")
    if(jobs LESS 1)
        set(jobs 1)
    elseif(jobs GREATER ${core_count})
        set(jobs ${core_count})
    endif()
    set(${out_var} ${jobs} PARENT_SCOPE)
endfunction()

if(DEFINED CMAKE_JOB_POOLS OR DEFINED CMAKE_JOB_POOL_COMPILE OR DEFINED CMAKE_JOB_POOL_LINK)
    # Someone has already described the pools they want; two schemes fighting
    # over the same knobs is worse than either.
    message(STATUS "Build parallelism: using caller-provided job pools")
elseif(MG_LIMIT_PARALLELISM_BY_MEMORY)
    if(NOT CMAKE_GENERATOR MATCHES "Ninja")
        message(WARNING
            "Job pools need a Ninja generator; with '${CMAKE_GENERATOR}' the build "
            "runs at whatever -j it is given and may exhaust memory.")
    else()
        cmake_host_system_information(RESULT mg_cores QUERY NUMBER_OF_LOGICAL_CORES)
        math(EXPR mg_cores "${mg_cores} - ${MG_RESERVE_CORES}")
        if(mg_cores LESS 1)
            set(mg_cores 1)
        endif()
        _mg_memory_budget_mb(mg_budget_mb)

        if(MG_COMPILE_JOBS GREATER 0)
            set(mg_compile_jobs ${MG_COMPILE_JOBS})
        else()
            _mg_jobs_for_budget(mg_compile_jobs
                                ${mg_budget_mb} ${MG_MEMORY_PER_COMPILE_JOB_MB} ${mg_cores})
        endif()

        if(MG_LINK_JOBS GREATER 0)
            set(mg_link_jobs ${MG_LINK_JOBS})
        else()
            _mg_jobs_for_budget(mg_link_jobs
                                ${mg_budget_mb} ${MG_MEMORY_PER_LINK_JOB_MB} ${mg_cores})
        endif()

        set_property(GLOBAL PROPERTY JOB_POOLS
                     mg_compile=${mg_compile_jobs} mg_link=${mg_link_jobs})
        set(CMAKE_JOB_POOL_COMPILE mg_compile)
        set(CMAKE_JOB_POOL_LINK mg_link)
        message(STATUS
            "Build parallelism: ${mg_compile_jobs} compile / ${mg_link_jobs} link "
            "(${mg_budget_mb} MiB usable, ${mg_cores} cores usable)")
    endif()
endif()
