import logging
import signal
import socket
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path


def is_port_free(port: int, host: str = "127.0.0.1") -> bool:
    """Check if a TCP port on the given host is free (not in use)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.01)
        result = sock.connect_ex((host, port))
        return result != 0  # If connect_ex returns 0, port is in use


def wait_for_server(
    proc: subprocess.Popen, port: int, host: str = "127.0.0.1", delay: float = 0.1, timeout: float = 10.0
):
    """Wait until server is accepting TCP connections on the given port, while the process is alive."""
    start_time = time.time()

    while True:
        # If the server has exited early, fail fast
        if proc.poll() is not None:
            raise RuntimeError("Process exited before accepting connections.")

        try:
            with socket.create_connection((host, port), timeout=1):
                time.sleep(delay)
                return
        except OSError:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Server did not start on port {port} within {timeout} seconds.")
            time.sleep(0.01)


@contextmanager
def memgraph_server(memgraph, data_dir: Path, port, logger, extra_args=None, timeout=10):
    """Context manager for managing the Memgraph server lifecycle."""

    if not is_port_free(port):
        raise RuntimeError(f"Port {port} is already in use. Cannot start memgraph server.")

    memgraph_cmd = [
        str(memgraph),
        "--storage-properties-on-edges",
        "--log-level=TRACE",
        "--also-log-to-stderr",
        f"--bolt-port={port}",
        f"--data-directory={data_dir.resolve()}",
    ] + (extra_args if extra_args else [])

    logger.info("Starting Memgraph server...")
    memgraph_proc = subprocess.Popen(memgraph_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        # Wait for the server to be ready
        wait_for_server(memgraph_proc, port)
        yield memgraph_proc  # Give control back to the caller within the context
    finally:
        stdout, stderr = None, None

        # Cleanup: send SIGINT to Memgraph to shut it down
        logger.info("Shutting down Memgraph server...")
        memgraph_proc.send_signal(signal.SIGINT)

        try:
            stdout, stderr = memgraph_proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            logger.error("Memgraph process is still running after terminate, force kill.")
            memgraph_proc.kill()
            try:
                stdout, stderr = memgraph_proc.communicate(timeout=3)
                raise RuntimeError(
                    f"Memgraph process had to be killed, durability maybe incorrect\n\nStdout:\n{stdout.decode()}\n\nStderr:\n{stderr.decode()}"
                )
            except subprocess.TimeoutExpired:
                raise RuntimeError("Memgraph process is in bad state and could not be killed.")

        if memgraph_proc.returncode is None:
            raise RuntimeError("Memgraph process did not terminate properly (possibly hung after kill).")

        if memgraph_proc.returncode != 0:
            raise RuntimeError(
                f"Memgraph exited with non-zero return code: {memgraph_proc.returncode}\n\nStdout:\n{stdout.decode()}\n\nStderr:\n{stderr.decode()}"
            )

        logger.info(f"Memgraph process finished with return code: {memgraph_proc.returncode}")
