import argparse
import os
import signal
import sys

from continuous_integration import BUILD_DIR, MemgraphRunner

"""
Run me like this:

```bash
cd tests/gql_behave
source ve3/bin/activate
python3 spam_script.py --runs 1000
```

It will load memgraph and stop it repeatedly until it crashes.
"""


def generate_core_dump():
    """Generate a core dump of the current process"""
    print("\nGenerating core dump...", flush=True)
    try:
        # Send SIGQUIT to current process to generate core dump
        os.kill(os.getpid(), signal.SIGQUIT)
    except Exception as e:
        print(f"Failed to generate core dump: {e}", flush=True)


def main(runs=1000):
    for run in range(runs):
        print(f"Running {run} of {runs}", flush=True)
        runner = MemgraphRunner(BUILD_DIR)

        try:
            runner.start()
        except Exception as e:
            print(f"FAILURE at run {run}: runner.start() failed with: {e}", flush=True)
            generate_core_dump()
            break

        try:
            runner.stop()
        except Exception as e:
            print(f"FAILURE at run {run}: runner.stop() failed with: {e}", flush=True)
            generate_core_dump()
            break
    else:
        print(f"Done! {runs} runs completed", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=1000)
    args = parser.parse_args()
    main(args.runs)
