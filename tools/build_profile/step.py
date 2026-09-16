#!/usr/bin/env python3
"""Run one build step and append its resource usage to a JSON-lines log.

CMake's RULE_LAUNCH_* properties (armed by launcher.cmake) put this in front of
every compile, link and custom command. Peak memory comes from two sources: the
kernel's rusage of the finished process tree, which is exact but reports the
largest single process, and a sampler thread that sums the RSS of every live
process in the tree, which catches a compiler driver plus its cc1 child or
cargo's rustc fan-out. Failures to log never fail the step.

    step.py --log FILE [--sample-ms N] --kind K [--target T] [--language L]
            [--target-type TT] [--source S] [--output O] -- COMMAND...
"""
import errno
import json
import os
import signal
import sys
import threading
import time

PAGE_KB = os.sysconf("SC_PAGE_SIZE") // 1024
OPTIONS = ("log", "sample_ms", "kind", "target", "language", "target_type", "source", "output")


def parse(argv):
    opts = {name: "" for name in OPTIONS}
    opts["sample_ms"] = "50"
    i = 1
    while i < len(argv):
        arg = argv[i]
        if arg == "--":
            return opts, argv[i + 1 :]
        key = arg[2:].replace("-", "_")
        if not arg.startswith("--") or key not in OPTIONS:
            sys.exit(f"step.py: unknown argument {arg!r}")
        # A placeholder that expanded to nothing leaves the next option in the
        # value's place; keep the value empty rather than swallowing the option.
        nxt = argv[i + 1] if i + 1 < len(argv) else "--"
        if nxt == "--" or (nxt.startswith("--") and nxt[2:].replace("-", "_") in OPTIONS):
            i += 1
        else:
            opts[key] = nxt
            i += 2
    sys.exit("step.py: missing '--' before the command")


def real_executable(cmd):
    """The program that does the work, seen past `env K=V ...` and `ccache`."""
    via_ccache = False
    for tok in cmd:
        base = os.path.basename(tok)
        if base == "env" or "=" in tok:
            continue
        if base == "ccache":
            via_ccache = True
            continue
        return base, via_ccache
    return (os.path.basename(cmd[0]) if cmd else ""), via_ccache


class TreeSampler(threading.Thread):
    """Periodically sum RSS over the child and all its descendants."""

    def __init__(self, pid, interval_s):
        super().__init__(daemon=True)
        self.pid = pid
        self.interval_s = interval_s
        self.stop = threading.Event()
        self.peak_rss_kb = 0
        self.peak_anon_kb = 0
        self.peak_procs = 0
        self.samples = 0

    def run(self):
        while True:
            self.sample()
            if self.stop.wait(self.interval_s):
                return

    def sample(self):
        pids = [self.pid]
        rss = anon = 0
        i = 0
        while i < len(pids):
            pid = pids[i]
            i += 1
            try:
                with open(f"/proc/{pid}/statm") as f:
                    fields = f.read().split()
                resident, shared = int(fields[1]), int(fields[2])
                rss += resident
                anon += resident - shared
                for tid in os.listdir(f"/proc/{pid}/task"):
                    try:
                        with open(f"/proc/{pid}/task/{tid}/children") as f:
                            pids.extend(int(c) for c in f.read().split())
                    except OSError:
                        pass
            except (OSError, ValueError, IndexError):
                pass
        self.samples += 1
        self.peak_rss_kb = max(self.peak_rss_kb, rss * PAGE_KB)
        self.peak_anon_kb = max(self.peak_anon_kb, anon * PAGE_KB)
        self.peak_procs = max(self.peak_procs, len(pids))


def main():
    opts, cmd = parse(sys.argv)
    if not cmd:
        sys.exit("step.py: empty command")

    started = time.time()
    mono0 = time.monotonic()
    pid = os.fork()
    if pid == 0:
        try:
            try:
                os.execvp(cmd[0], cmd)
            except OSError as e:
                # A script with no shebang: the shell would run it itself, so do the same.
                if e.errno != errno.ENOEXEC:
                    raise
                os.execv("/bin/sh", ["/bin/sh", *cmd])
        except OSError as e:
            sys.stderr.write(f"step.py: cannot execute {cmd[0]}: {e.strerror}\n")
            os._exit(127)

    # The child shares our process group and gets Ctrl-C itself; we only report
    # how it ended. TERM aimed at the wrapper is passed down instead.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, lambda *_: os.kill(pid, signal.SIGTERM))

    sampler = None
    sample_ms = int(opts["sample_ms"] or 0)
    if sample_ms > 0:
        sampler = TreeSampler(pid, sample_ms / 1000.0)
        sampler.start()

    _, status, ru = os.wait4(pid, 0)
    wall_s = time.monotonic() - mono0
    if sampler:
        sampler.stop.set()
        sampler.join()

    if os.WIFSIGNALED(status):
        code = 128 + os.WTERMSIG(status)
    else:
        code = os.WEXITSTATUS(status)

    exe, via_ccache = real_executable(cmd)
    kind = opts["kind"]
    if exe == "clang-scan-deps":
        kind = "scan"
    if not via_ccache:
        ccache = "off"
    elif os.environ.get("CCACHE_DISABLE"):
        ccache = "disabled"
    else:
        ccache = "on"

    # A custom command with several outputs names them all, comma-separated;
    # the first is label enough.
    outputs = opts["output"].split(",")
    rec = {
        "kind": kind,
        "target": opts["target"],
        "language": opts["language"],
        "target_type": opts["target_type"],
        "source": opts["source"],
        "output": outputs[0],
        "outputs": len(outputs),
        "exe": exe,
        "ccache": ccache,
        "start": round(started, 3),
        "end": round(started + wall_s, 3),
        "wall_s": round(wall_s, 4),
        "user_s": round(ru.ru_utime, 4),
        "sys_s": round(ru.ru_stime, 4),
        "maxrss_kb": ru.ru_maxrss,
        "tree_rss_peak_kb": sampler.peak_rss_kb if sampler else 0,
        "tree_anon_peak_kb": sampler.peak_anon_kb if sampler else 0,
        "tree_procs_peak": sampler.peak_procs if sampler else 0,
        "samples": sampler.samples if sampler else 0,
        "majflt": ru.ru_majflt,
        "minflt": ru.ru_minflt,
        "inblock": ru.ru_inblock,
        "oublock": ru.ru_oublock,
        "nvcsw": ru.ru_nvcsw,
        "nivcsw": ru.ru_nivcsw,
        "exit": code,
        "cwd": os.getcwd(),
    }
    if kind == "custom":
        rec["cmd"] = " ".join(cmd)[:300]

    try:
        line = (json.dumps(rec, separators=(",", ":")) + "\n").encode()
        fd = os.open(opts["log"], os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
        try:
            os.write(fd, line)
        finally:
            os.close(fd)
    except OSError as e:
        sys.stderr.write(f"step.py: could not append to {opts['log']}: {e}\n")
    sys.exit(code)


if __name__ == "__main__":
    main()
