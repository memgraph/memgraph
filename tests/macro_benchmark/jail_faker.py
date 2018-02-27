#!/usr/bin/python3
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

    def run(self, binary, args=None, env=None, timeout=120,
            stdin="/dev/null", cwd="."):
        if args is None: args = []
        if env is None: env = {}
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
        self._proc = subprocess.Popen(exe, env=env, cwd=cwd,
                stdin=open(stdin, "r"))

    def run_and_wait(self, *args, **kwargs):
        check = kwargs.pop("check", True)
        self.run(*args, **kwargs)
        return self.wait(check)

    def wait(self, check = True):
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
        sys.stderr.write("WARNING: Trying to set cpus for {} to "
                "{} with{} hyperthreading!\n".format(str(self), cpus, s))

    # this is implemented only in the real API
    def set_nproc(self, nproc):
        sys.stderr.write("WARNING: Trying to set nproc for {} to "
                "{}!\n".format(str(self), nproc))

    # this is implemented only in the real API
    def set_memory(self, memory):
        sys.stderr.write("WARNING: Trying to set memory for {} to "
                "{}\n".format(str(self), memory))

    # WARNING: this won't be implemented in the real API
    def get_pid(self):
        if self._proc == None:
            raise ProcessException
        return self._proc.pid

    def _set_usage(self, val, name, only_value=False):
        self._usage[name] = val
        if only_value: return
        maxname = "max_" + name
        maxval = val
        if maxname in self._usage:
            maxval = self._usage[maxname]
        self._usage[maxname] = max(maxval, val)

    def _do_background_tasks(self):
        self._update_usage()
        self._watchdog()

    def _update_usage(self):
        if self._proc == None: return
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
        utime, stime, cutime, cstime = map(
                lambda x: int(x) / self._ticks_per_sec, data_stat[13:17])
        self._set_usage(utime + stime + cutime + cstime, "cpu", only_value=True)
        self._set_usage(utime + cutime, "cpu_user", only_value=True)
        self._set_usage(stime + cstime, "cpu_sys", only_value=True)
        self._set_usage(int(data_stat[19]), "threads")
        mem_vm, mem_res, mem_shr = map(
                lambda x: int(x) * self._page_size // 1024, data_statm[:3])
        self._set_usage(mem_res, "memory")

    def _watchdog(self):
        if self._proc == None or self._proc.returncode != None: return
        if time.time() - self._start_time < self._timeout: return
        sys.stderr.write("Timeout of {}s reached, sending "
                "SIGKILL to {}!\n".format(self._timeout, self))
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
        if proc._proc == None: continue
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
        if not row: continue
        data = row.split("\n")
        core_id, physical_id = -1, -1
        for line in data:
            name, val = map(lambda x: x.strip(), line.split(":"))
            if name == "physical id": physical_id = int(val)
            elif name == "core id": core_id = int(val)
        threads += 1
        cpus.add((core_id, physical_id))
    cpus = len(cpus)

    hyper = True if cpus != threads else False

    return {"cpus": cpus, "memory": memory, "hyperthreading": hyper,
            "threads": threads}

# placeholder function that stores a label in the real API
def store_label(label):
    if type(label) != str:
        raise Exception("Label must be a string!")

# this function is deprecated
def store_data(data):
    pass

# placeholder function that returns real data in the real API
def get_network_usage():
    usage = {
        "lo": {
            "bytes": {"rx": 0, "tx": 0},
            "packets": {"rx": 0, "tx": 0}
        },
        "eth0": {
            "bytes": {"rx": 0, "tx": 0},
            "packets": {"rx": 0, "tx": 0}
        }
    }
    return usage
