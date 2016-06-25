#pragma mark

#include "sys/types.h"
#include "sys/sysinfo.h"

auto total_virtual_memory()
{
	struct sysinfo mem_info;
	sysinfo (&mem_info);
	long long total_virtual_memory = mem_info.totalram;
	total_virtual_memory += mem_info.totalswap;
	total_virtual_memory *= mem_info.mem_unit;
	return total_virtual_memory;
}

auto used_virtual_memory()
{
	struct sysinfo mem_info;
	sysinfo (&mem_info);
    long long virtual_memory_used = mem_info.totalram - mem_info.freeram;
    virtual_memory_used += mem_info.totalswap - mem_info.freeswap;
    virtual_memory_used *= mem_info.mem_unit;
    return virtual_memory_used;
}
