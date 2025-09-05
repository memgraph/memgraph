#!/usr/bin/env python3
import argparse
import os
import sys
import time
import tempfile
import datetime

import interactive_mg_runner


def wait_for_instance_ready(instance_name, max_wait=10):
    """Wait for Memgraph instance to be ready using interactive_mg_runner API."""
    print(f"Waiting for Memgraph instance '{instance_name}' to be ready...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        if instance_name in interactive_mg_runner.MEMGRAPH_INSTANCES:
            instance = interactive_mg_runner.MEMGRAPH_INSTANCES[instance_name]
            if instance.is_running():
                print(f"Instance is ready after {time.time() - start_time:.2f} seconds")
                return True
        time.sleep(0.5)
    
    print(f"Instance failed to become ready within {max_wait} seconds")
    return False


def run_single_test(args, repo_root, run_number):
    """Run a single test iteration."""
    # Create unique testing directory with timestamp and run number
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Include milliseconds
    testing_dir = os.path.join(repo_root, "tests", "e2e", "testing", f"run_{timestamp}_#{run_number}")
    os.makedirs(testing_dir, exist_ok=True)
    
    print(f"Run #{run_number} - Using testing directory: {testing_dir}")

    instance_name = args.name
    log_file = os.path.join(testing_dir, f"{instance_name}.log")
    data_directory = os.path.join(testing_dir, "data")
    
    context = {
        instance_name: {
            "args": ["--bolt-port", str(args.bolt_port), f"--log-level={args.log_level}"],
            "log_file": log_file,
            "data_directory": data_directory,
        }
    }
    
    if args.data_directory:
        context[instance_name]["data_directory"] = os.path.join(testing_dir, args.data_directory)

    try:
        print(f"Starting Memgraph instance '{instance_name}' on port {args.bolt_port}...")
        interactive_mg_runner.start(context, instance_name)
        
        # Wait for instance to be ready
        if not wait_for_instance_ready(instance_name):
            raise RuntimeError(f"Instance failed to become ready within 10 seconds")
        
        print(f"Instance is running.")
        print("Stopping instance...")
        interactive_mg_runner.stop(context, instance_name, keep_directories=args.keep_directories)
        print(f"Run #{run_number} completed successfully.")
        
        # Clean up successful run directory
        import shutil
        #shutil.rmtree(testing_dir)
        print(f"Cleaned up testing directory: {testing_dir}")
        return True
        
    except Exception as e:
        print(f"ERROR in run #{run_number}: {e}")
        print(f"Check logs in: {testing_dir}")
        print(f"Log file: {log_file}")
        if os.path.exists(log_file):
            print("Last 20 lines of log file:")
            try:
                with open(log_file, 'r') as f:
                    lines = f.readlines()
                    for line in lines[-20:]:
                        print(f"  {line.rstrip()}")
            except Exception as log_e:
                print(f"Could not read log file: {log_e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Start and stop a Memgraph instance using interactive_mg_runner in a loop")
    parser.add_argument("--name", default="single", help="Instance name")
    parser.add_argument("--bolt-port", type=int, default=7687, help="Bolt port to use")
    parser.add_argument("--sleep", type=float, default=5.0, help="Seconds to keep Memgraph running before stopping")
    parser.add_argument("--keep-directories", action="store_true", help="Keep data directories on stop")
    parser.add_argument("--log-level", default="TRACE", help="Memgraph log level")
    parser.add_argument("--data-directory", default="", help="Custom data directory name (optional)")
    args = parser.parse_args()

    repo_root = os.getcwd()
    
    # Setup interactive_mg_runner paths
    sys.path.insert(0, os.path.join(repo_root, "tests", "e2e"))
    interactive_mg_runner.SCRIPT_DIR = os.path.join(repo_root, "tests", "e2e")
    interactive_mg_runner.PROJECT_DIR = repo_root
    interactive_mg_runner.BUILD_DIR = os.path.join(repo_root, "build")
    interactive_mg_runner.MEMGRAPH_BINARY = os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph")

    run_number = 1
    print("Starting continuous Memgraph instance testing...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            success = run_single_test(args, repo_root, run_number)
            if not success:
                print(f"Test failed on run #{run_number}. Stopping.")
                sys.exit(1)
            run_number += 1
            print(f"Completed {run_number - 1} successful runs. Starting run #{run_number}...")
            
    except KeyboardInterrupt:
        print(f"\nStopped by user after {run_number - 1} successful runs.")
        sys.exit(0)


if __name__ == "__main__":
    main()
