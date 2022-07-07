#!/usr/bin/python3

# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import atexit
import copy
import json
import os
import resource
import shutil
import subprocess
import sys
import threading
import time
import uuid
from signal import *


class ProcessException(Exception):
    pass


class StorageException(Exception):
    pass


class Process:
    def __init__(self, tid):
        self._tid = tid
        self._proc = None
        self._ticks_per_sec = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        self._page_size = os.sysconf(os.sysconf_names["SC_PAGESIZE"])
        self._usage = {}
        self._files = []

    def run(self, binary, args=None, env=None, timeout=120, stdin="/dev/null", cwd="."):
        if args is None:
            args = []
        if env is None:
            env = {}
        # don't start a new process if one is already running
        if self._proc != None and self._proc.returncode == None:
            raise ProcessException

        # clear previous usage
        self._usage = {}

        # create exe list
        exe = [binary] + args

        # set environment variables
        keys = ["PATH", "HOME", "USER", "LANG", "PWD"]
        for key in keys:
            env[key] = os.environ[key]

        # set start time
        self._start_time = time.time()
        self._timeout = timeout

        # start process
        self._proc = subprocess.Popen(exe, env=env, cwd=cwd, stdin=open(stdin, "r"))

    def run_and_wait(self, *args, **kwargs):
        check = kwargs.pop("check", True)
        self.run(*args, **kwargs)
        return self.wait(check)

    def wait(self, check=True):
        if self._proc == None:
            raise ProcessException
        self._proc.wait()
        if check and self._proc.returncode != 0:
            raise ProcessException("Command returned non-zero!")
        return self._proc.returncode

    def get_status(self):
        if self._proc == None:
            raise ProcessException
        self._proc.poll()
        return self._proc.returncode

    def send_signal(self, signal):
        if self._proc == None:
            raise ProcessException
        self._proc.send_signal(signal)

    def get_usage(self):
        if self._proc == None:
            raise ProcessException
        return copy.deepcopy(self._usage)

    # this is implemented only in the real API
    def set_cpus(self, cpus, hyper=True):
        s = "out" if not hyper else ""
        sys.stderr.write(
            "WARNING: Trying to set cpus for {} to " "{} with{} hyperthreading!\n".format(str(self), cpus, s)
        )

    # this is implemented only in the real API
    def set_nproc(self, nproc):
        sys.stderr.write("WARNING: Trying to set nproc for {} to " "{}!\n".format(str(self), nproc))

    # this is implemented only in the real API
    def set_memory(self, memory):
        sys.stderr.write("WARNING: Trying to set memory for {} to " "{}\n".format(str(self), memory))

    # WARNING: this won't be implemented in the real API
    def get_pid(self):
        if self._proc == None:
            raise ProcessException
        return self._proc.pid

    def _set_usage(self, val, name, only_value=False):
        self._usage[name] = val
        if only_value:
            return
        maxname = "max_" + name
        maxval = val
        if maxname in self._usage:
            maxval = self._usage[maxname]
        self._usage[maxname] = max(maxval, val)

    def _do_background_tasks(self):
        self._update_usage()
        self._watchdog()

    def _update_usage(self):
        if self._proc == None:
            return
        try:
            f = open("/proc/{}/stat".format(self._proc.pid), "r")
            data_stat = f.read().split()
            f.close()
            f = open("/proc/{}/statm".format(self._proc.pid), "r")
            data_statm = f.read().split()
            f.close()
        except:
            return
        # for a description of these fields see: man proc; man times
        utime, stime, cutime, cstime = map(lambda x: int(x) / self._ticks_per_sec, data_stat[13:17])
        self._set_usage(utime + stime + cutime + cstime, "cpu", only_value=True)
        self._set_usage(utime + cutime, "cpu_user", only_value=True)
        self._set_usage(stime + cstime, "cpu_sys", only_value=True)
        self._set_usage(int(data_stat[19]), "threads")
        mem_vm, mem_res, mem_shr = map(lambda x: int(x) * self._page_size // 1024, data_statm[:3])
        self._set_usage(mem_res, "memory")

    def _watchdog(self):
        if self._proc == None or self._proc.returncode != None:
            return
        if time.time() - self._start_time < self._timeout:
            return
        sys.stderr.write("Timeout of {}s reached, sending " "SIGKILL to {}!\n".format(self._timeout, self))
        self.send_signal(SIGKILL)
        self.get_status()

    def __repr__(self):
        return "Process(id={})".format(self._tid)


# private methods -------------------------------------------------------------

PROCESSES_NUM = 8
_processes = [Process(i) for i in range(1, PROCESSES_NUM + 1)]
_last_process = 0


def _usage_updater():
    while True:
        for proc in _processes:
            proc._do_background_tasks()
        time.sleep(0.1)


_thread = threading.Thread(target=_usage_updater, daemon=True)
_thread.start()


@atexit.register
def cleanup():
    for proc in _processes:
        if proc._proc == None:
            continue
        proc.send_signal(SIGKILL)
        proc.get_status()


# end of private methods ------------------------------------------------------


def get_process():
    global _last_process
    if _last_process < PROCESSES_NUM:
        proc = _processes[_last_process]
        _last_process += 1
        return proc
    return None


def get_host_info():
    with open("/proc/meminfo") as f:
        memdata = f.read()

    memory = 0
    for row in memdata.split("\n"):
        tmp = row.split()
        if tmp[0] == "MemTotal:":
            memory = int(tmp[1])
            break

    with open("/proc/cpuinfo") as f:
        cpudata = f.read()

    threads, cpus = 0, set()
    for row in cpudata.split("\n\n"):
        if not row:
            continue
        data = row.split("\n")
        core_id, physical_id = -1, -1
        for line in data:
            name, val = map(lambda x: x.strip(), line.split(":"))
            if name == "physical id":
                physical_id = int(val)
            elif name == "core id":
                core_id = int(val)
        threads += 1
        cpus.add((core_id, physical_id))
    cpus = len(cpus)

    hyper = True if cpus != threads else False

    return {"cpus": cpus, "memory": memory, "hyperthreading": hyper, "threads": threads}


# placeholder function that stores a label in the real API
def store_label(label):
    if type(label) != str:
        raise Exception("Label must be a string!")


"""
This function flushes network rules from the filter table.

The parameter `chain` can either be None, "INPUT" or "OUTPUT".

If chain is None, this function performs the following commands:
    ```
    iptables -P INPUT ACCEPT
    iptables -P OUTPUT ACCEPT
    iptables -F INPUT
    iptables -F OUTPUT
    ```

If chain is either "INPUT" or "OUTPUT" then only that chain is cleared using
the appropriate subset of the above mentioned commands.
"""


def network_flush_rules(chain=None):
    print("Network flush rules: chain={}".format(chain))


"""
This function blocks TCP packets using the iptables firewall.

The parameter `chain` must be either "INPUT" or "OUTPUT".

The parameters `src` and `dst` must be addresses and exactly one of them must
be always specified.

The parameters `sport` and `dport` must be ports used for TCP filtering and
exactly one of them must be always specified.

The parameter `action` must be either "DROP" or "REJECT".

If `src` and `sport` are specified, this function performs the following
command:
    `iptables -A $chain -s $src -p tcp --sport $sport -j $action`
The same principle is valid for all other combinations of `src`, `dst`, `sport`
and `dport`.

The journey of a network packet from its source to its destination is described
in the following diagram:

    +--------------------+        request        +-------------------+
    | client             |  1 ->--->--->--->- 2  | server            |
    |                    |                       |                   |
    | addr: 192.168.1.50 |                       | addr: 192.168.1.1 |
    | port: unknown      |  4 -<---<---<---<- 3  | port: 1000        |
    +--------------------+        response       +-------------------+

    From the diagram, we can see that to identify a network stream, we only
    have the following parameters about it that we can use for filtering:
    `addr_client`, `addr_server` and `port_server`.

    To block the stream in points of transfer (1, 2, 3, 4), you need to issue
    the following command (on the specific machine):
        1. block on CLIENT: chain=OUTPUT, dst=addr_server, dport=port_server
        2. block on SERVER: chain=INPUT,  src=addr_client, dport=port_server
        3. block on SERVER: chain=OUTPUT, dst=addr_client, sport=port_server
        4. block on CLIENT: chain=INPUT,  src=addr_server, sport=port_server

    Other combinations of `chain`, `src`/`dst` and `sport`/`dport` can be used,
    but are advised to be used only when you exactly know what you are doing :)
"""


def network_block_tcp(chain=None, src=None, dst=None, sport=None, dport=None, action=None):
    print(
        "Network block TCP: chain={}, src={}, dst={}, sport={}, dport={}, "
        "action={}".format(chain, src, dst, sport, dport, action)
    )


"""
This function unblocks TCP packets using the iptables firewall.

It is used to remove rules added by the `network_block_tcp` function.

Rules that are added can be removed by calling this function with *exactly the
same* parameters that were used to define the rule in the first place.

All other documentation for this function is the same as for
`network_block_tcp`, so take a look there.
"""


def network_unblock_tcp(chain=None, src=None, dst=None, sport=None, dport=None, action=None):
    print(
        "Network unblock TCP: chain={}, src={}, dst={}, sport={}, dport={}, "
        "action={}".format(chain, src, dst, sport, dport, action)
    )


# this function is deprecated
def store_data(data):
    pass


# placeholder function that returns real data in the real API
def get_network_usage():
    usage = {
        "lo": {"bytes": {"rx": 0, "tx": 0}, "packets": {"rx": 0, "tx": 0}},
        "eth0": {"bytes": {"rx": 0, "tx": 0}, "packets": {"rx": 0, "tx": 0}},
    }
    return usage
