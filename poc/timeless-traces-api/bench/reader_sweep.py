#!/usr/bin/env python3
"""Run the fixed query matrix with one, two, four, and eight readers."""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request


def wait_ready(url, process):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read().decode(errors="replace")
            raise RuntimeError(f"server exited {process.returncode}: {stderr}")
        try:
            with urllib.request.urlopen(url + "/ready", timeout=1) as response:
                if response.status == 200:
                    return
        except Exception:
            time.sleep(0.01)
    raise RuntimeError("server did not become ready")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--extension", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--url", default="http://127.0.0.1:19449")
    parser.add_argument("--repeats", type=int, default=20)
    args = parser.parse_args()
    results = {}
    bench_dir = os.path.dirname(os.path.abspath(__file__))
    for readers in (1, 2, 4, 8):
        environment = os.environ.copy()
        environment.update({
            "TIMELESS_TRACES_READER_CONNECTIONS": str(readers),
            "TIMELESS_TRACES_RETENTION_SECS": "0",
            "TIMELESS_TRACES_FLUSH_INTERVAL_SECS": "3600",
            "TIMELESS_TRACES_OPTIMIZE_INTERVAL_SECS": "3600",
        })
        process = subprocess.Popen(
            [args.binary, args.extension, args.database, "127.0.0.1:19449"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
        )
        try:
            wait_ready(args.url, process)
            completed = subprocess.run(
                [
                    sys.executable,
                    os.path.join(bench_dir, "query.py"),
                    "--url",
                    args.url,
                    "--server-pid",
                    str(process.pid),
                    "--repeats",
                    str(args.repeats),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            results[str(readers)] = json.loads(completed.stdout)
        finally:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
            try:
                process.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate()
                raise RuntimeError("server did not drain after SIGTERM")
            if process.returncode != 0:
                stderr = process.stderr.read().decode(errors="replace")
                raise RuntimeError(f"server exited {process.returncode}: {stderr}")
    print(json.dumps({"reader_sweep": results}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
