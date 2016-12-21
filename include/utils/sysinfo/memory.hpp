#pragma mark

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "sys/sysinfo.h"
#include "sys/types.h"

auto total_virtual_memory()
{
    struct sysinfo mem_info;
    sysinfo(&mem_info);
    long long total_virtual_memory = mem_info.totalram;
    total_virtual_memory += mem_info.totalswap;
    total_virtual_memory *= mem_info.mem_unit;
    return total_virtual_memory;
}

auto used_virtual_memory()
{
    struct sysinfo mem_info;
    sysinfo(&mem_info);
    long long virtual_memory_used = mem_info.totalram - mem_info.freeram;
    virtual_memory_used += mem_info.totalswap - mem_info.freeswap;
    virtual_memory_used *= mem_info.mem_unit;
    return virtual_memory_used;
}

// TODO: OS dependent

/**
 * parses memory line from /proc/self/status
 */
auto parse_vm_size(char *line)
{
    // This assumes that a digit will be found and the line ends in " Kb".
    auto i        = std::strlen(line);
    const char *p = line;
    while (*p < '0' || *p > '9')
        p++;
    line[i - 3] = '\0';
    return std::atoll(p);
}

/**
 * returns VmSize in kB
 */
auto vm_size()
{
    std::FILE *file = std::fopen("/proc/self/status", "r");
    auto result     = -1LL;
    char line[128];

    while (fgets(line, 128, file) != NULL)
    {
        if (strncmp(line, "VmSize:", 7) == 0)
        {
            result = parse_vm_size(line);
            break;
        }
    }

    fclose(file);

    return result;
}
