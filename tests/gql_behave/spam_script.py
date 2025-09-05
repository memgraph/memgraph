from continuous_integration import MemgraphRunner
import argparse
import signal
import os
import sys

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
        print(f"\rRunning {run} of {runs}", end="", flush=True)
        runner = MemgraphRunner()
        
        try:
            runner.start()
        except Exception as e:
            print(f"\nFAILURE at run {run}: runner.start() failed with: {e}", flush=True)
            generate_core_dump()
            break
            
        try:
            runner.stop()
        except Exception as e:
            print(f"\nFAILURE at run {run}: runner.stop() failed with: {e}", flush=True)
            generate_core_dump()
            break
    else:
        print(f"\nDone! {runs} runs completed", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=1000)
    args = parser.parse_args()
    main(args.runs)