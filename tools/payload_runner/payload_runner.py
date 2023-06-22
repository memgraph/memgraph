#!/usr/bin/env python3

import argparse
import os
import signal
import subprocess
import sys
import time

c_red = "\033[91m"
c_green = "\033[92m"
c_yellow = "\033[93m"
c_blue = "\033[94m"
c_reset = "\033[0m"

no_color = False


def send(mgconsole, cmd):
    process = subprocess.Popen(
        [mgconsole], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    output, error = process.communicate(input=cmd)
    ret = process.wait()
    return output, error, ret


def get_actual_pid(pid):
    pgrep_cmd = ["pgrep", "-P", str(pid)]
    pgrep_process = subprocess.Popen(pgrep_cmd, stdout=subprocess.PIPE)
    output, _ = pgrep_process.communicate()
    child_pid = int(output.strip())
    return child_pid


def program_exists(prog):
    process = subprocess.run(["which", prog], capture_output=True)
    return process.returncode == 0


def error(msg):
    if no_color:
        print(msg)
    else:
        print(f"{c_red}{msg}{c_reset}")
    sys.exit(1)


def print_file_content(content):
    if len(content) > 300:
        content = content[:300] + "\n<snipped>"
    if no_color:
        print(content)
    else:
        print(f"{c_green}{content.strip()}{c_reset}")
    print()


def validate(args):
    # test_file
    if not os.path.exists(args.test_file):
        error(f"No such test file: {args.test_file}")
    # setup_file
    if args.setup_file and not os.path.exists(args.setup_file):
        error(f"No such setup file: {args.setup_file}")
    # mgconsole
    if not program_exists(args.mgconsole):
        error(f"mgconsole not found: {args.mgconsole}")
    # memgraph
    if not program_exists(args.memgraph):
        error(f"memgraph not found: {args.memgraph}")


def main(args, custom_env):
    validate(args)

    # launch memgraph
    memgraph_process = subprocess.Popen(
        [args.memgraph],
        shell=True,
        stdout=subprocess.PIPE,
        env=custom_env,
    )

    print("Waiting for memgraph to be responsive...", end="", flush=True)
    memgraph_ready = False
    while not memgraph_ready:
        _, _, ret = send(args.mgconsole, "")
        if not ret:
            memgraph_ready = True
        else:
            time.sleep(0.4)
            print(".", end="", flush=True)
    print(" Ready\n")

    # I think process forks for some reason, need to get the child's pid
    pid = get_actual_pid(memgraph_process.pid)

    try:
        # setup
        if args.setup_file:
            print(f"Setup command from: '{args.setup_file}'")
            with open(args.setup_file, "r") as setup_file:
                setup_content = setup_file.read()
                print_file_content(setup_content)

                print("Running setup...", end="", flush=True)
                send(args.mgconsole, setup_content)
                print(" Done\n")

        # run test
        print(f"Test command from: '{args.test_file}'")
        with open(args.test_file, "r") as test_file:
            test_content = test_file.read()
            print_file_content(test_content)

        # TODO: replace with auto connect of whatever tool
        print(f"Will run for {args.nruns} runs")
        input("Press Enter to continue...")
        print()

        print("Running test command", end="", flush=True)
        for _ in range(args.nruns):
            print(".", end="", flush=True)
            send(args.mgconsole, test_content)
        print(" Done\n")

    finally:
        os.kill(pid, signal.SIGTERM)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Perf harness for memgraph")

    parser.add_argument(
        "--mgconsole", type=str, help="The mgconsole binary to use", default=os.environ.get("MGCONSOLE") or "mgconsole"
    )
    parser.add_argument(
        "--memgraph", type=str, help="The memgraph binary to use", default=os.environ.get("MEMGRAPH") or "memgraph"
    )
    parser.add_argument(
        "--memgraph_config",
        type=str,
        help="Config file to use for memgraph",
    )
    parser.add_argument("--setup_file", type=str, help="Setup file", default="")
    parser.add_argument("test_file", type=str, help="Test file")
    parser.add_argument("--nruns", type=int, help="Number of runs", default=1)
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable color output",
    )

    args = parser.parse_args()
    no_color = args.no_color
    custom_env = dict(os.environ)
    if args.memgraph_config:
        custom_env["MEMGRAPH_CONFIG"] = args.memgraph_config
    main(args, custom_env)
