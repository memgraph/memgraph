#!/usr/bin/python3
import atexit
import os
import shutil
import subprocess
import sys
import threading
import time
import uuid
from signal import *
import datetime
import json


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
TEMP_DIR = os.path.join(SCRIPT_DIR, ".temp")
STORAGE_DIR = os.path.join(SCRIPT_DIR, ".storage")


class ProcessException(Exception):
    pass


class Process:
    def __init__(self, tid):
        self._tid = tid
        self._proc = None
        self._ticks_per_sec = os.sysconf(os.sysconf_names["SC_CLK_TCK"])
        self._page_size = os.sysconf(os.sysconf_names["SC_PAGESIZE"])
        self._usage = {}
        self._files = []

    def run(self, binary, args=[], env={}, timeout=10000, stdin="/dev/null"):
        # don't start a new process if one is already running
        if self._proc is not None and self._proc.returncode is None:
            raise ProcessException

        # clear previous usage
        self._usage = {}

        # create exe list
        exe = [binary] + args

        # temporary stdout and stderr files
        stdout = self._temporary_file("stdout")
        stderr = self._temporary_file("stderr")
        self._files = [stdout, stderr]

        # set environment variables
        keys = ["PATH", "HOME", "USER", "LANG", "PWD"]
        for key in keys:
            env[key] = os.environ[key]

        # set start time
        self._start_time = time.time()
        self._timeout = timeout

        # start process
        self._proc = subprocess.Popen(exe, env = env,
                stdin = open(stdin, "r"),
                stdout = open(stdout, "w"),
                stderr = open(stderr, "w"))

    def wait(self):
        if self._proc == None:
            raise ProcessException()
        self._proc.wait()
        return self._proc.returncode

    def run_and_wait(self, *args, **kwargs):
        self.run(*args, **kwargs)
        return self.wait()

    def get_pid(self):
        if self._proc == None:
            raise ProcessException
        return self._proc.pid

    def get_usage(self):
        if self._proc == None:
            raise ProcessException
        return self._usage

    def get_status(self):
        if self._proc == None:
            raise ProcessException
        self._proc.poll()
        return self._proc.returncode

    def get_stdout(self):
        if self._proc == None or self._proc.returncode == None:
            raise ProcessException
        return self._files[0]

    def get_stderr(self):
        if self._proc == None or self._proc.returncode == None:
            raise ProcessException
        return self._files[1]

    def send_signal(self, signal):
        if self._proc == None:
            raise ProcessException
        self._proc.send_signal(signal)

    def _temporary_file(self, name):
        return os.path.join(TEMP_DIR, ".".join([name, str(uuid.uuid4()), "dat"]))

    def _set_usage(self, val, name, only_value = False):
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
        cpu_time = sum(map(lambda x: int(x) / self._ticks_per_sec, data_stat[13:17]))
        self._set_usage(cpu_time, "cpu_usage", only_value = True)
        self._set_usage(int(data_stat[19]), "threads")
        mem_vm, mem_res, mem_shr = map(lambda x: int(x) * self._page_size // 1024, data_statm[:3])
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
_thread_run = True

def _usage_updater():
    while True:
        for proc in _processes:
            proc._do_background_tasks()
        time.sleep(0.1)

_thread = threading.Thread(target = _usage_updater, daemon = True)
_thread.start()

if not os.path.exists(STORAGE_DIR):
    os.mkdir(STORAGE_DIR)
if os.path.exists(TEMP_DIR):
    shutil.rmtree(TEMP_DIR)
os.mkdir(TEMP_DIR)

_storage_file = os.path.join(STORAGE_DIR, time.strftime("%Y%m%d%H%M%S") + ".json")

@atexit.register
def cleanup():
    for proc in _processes:
        if proc._proc == None: continue
        proc.send_signal(SIGKILL)
        proc.get_status()
    shutil.rmtree(TEMP_DIR)

# end of private methods ------------------------------------------------------


def get_process():
    global _last_process
    if _last_process < PROCESSES_NUM:
        proc = _processes[_last_process]
        _last_process += 1
        return proc
    return None

# TODO: ovo treba napravit
def store_data(_data, self):
    data = json.loads(_data)
    assert "unit" in data, "unit is nonoptional field"
    assert "type" in data, "type is nonoptional field"
    assert "value" in data, "value is nonoptional field"
    if not hasattr(self, "timestamp"):
        self.timestamp = datetime.datetime.now().isoformat()
    with open(os.path.join(os.path.dirname(__file__), "results",
        self.timestamp), "a") as results_file:
        json.dump(data, results_file)
        results_file.write("\n")


    # TODO: treba assertat da data ima neke keyeve u sebi
    # TODO: to trebaju bit keyevi value, type i sl...


    # unit - obavezno
    # type - obavezno
    # target - (setup, tardown, ...)
    # iter
    # value - obavezno
    # scenario
    # group
    # status ?
    # cpu_clock - obavezno?

    # timestamp - obavezno, automatski
    # query
    # engine
    # engine_config

    # TODO: store-aj ovo kao validni json
    # to znaci da treba bit lista dictionary-ja

    pass
store_data.__defaults__ = (store_data,)


if __name__ == "__main__":
    proc = get_process()
    proc.run("/home/matej/memgraph/jail/api/tester", timeout = 1500)
    #proc.run("/home/matej/memgraph/memgraph/build/memgraph_829_97638d3_dev_debug", env={"MEMGRAPH_CONFIG": "/home/matej/memgraph/memgraph/config/memgraph.yaml"})

    time.sleep( 1.000 )
    print("usage1:", proc.get_usage())
    print("status:", proc.get_status())
    #proc.send_signal(SIGTERM)
    cnt = 0
    while proc.get_status() == None:
        usage = proc.get_usage()
        usage_str = "    "
        for key in sorted(usage.keys()):
            usage_str += key + (": %10.3f" % usage[key]) + ";  "
        print(usage_str)
        time.sleep( 0.1 )
        cnt += 1
    proc.send_signal(SIGTERM)
    print("status", proc.get_status())
    print("usage2", proc.get_usage())
    while proc.get_status() == None:
        print("cekam da umre...")
        time.sleep( 0.1 )
    print("stdout 3:", proc.get_stdout())
