#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
import subprocess
import signal


class Perf():
    def __init__(self):
        self.first = True
        self.max_frequency = Path(
            "/proc/sys/kernel/perf_event_max_sample_rate").read_text().strip()
        # Check if lbr is available.
        status = subprocess.call(
                "perf record --call-graph=lbr -a -g sleep 0.0000001".split())
        self.call_graph_technique = "lbr" if not status else "dwarf"


    def start(self, pid, frequency=None):
        if frequency is None: frequency = self.max_frequency
        append = "-A" if not self.first else ""
        self.first = False
        perf_command = "perf record --call-graph={} -F {} -p {} -g {}".format(
                self.call_graph_technique, frequency, pid, append).split()
        self.perf_proc = subprocess.Popen(perf_command)


    def stop(self):
        self.perf_proc.send_signal(signal.SIGUSR1)
        self.perf_proc.wait()
