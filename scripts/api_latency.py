#!/usr/bin/env python3
"""Measure API latency percentiles under concurrent load.

Used by the load experiments in FINDINGS.md. Standard library only.

  ./scripts/api_latency.py --url http://localhost:8080/api/status --seconds 20
"""

import argparse
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request

def worker(url, stop_at, samples, errors, lock):
    local, failed = [], 0
    while time.monotonic() < stop_at:
        t0 = time.perf_counter()
        try:
            with urllib.request.urlopen(url, timeout=30) as r:
                r.read()
            local.append((time.perf_counter() - t0) * 1000.0)
        except (urllib.error.URLError, OSError, TimeoutError):
            failed += 1
    with lock:
        samples.extend(local)
        errors[0] += failed

def percentile(values, p):
    if not values:
        return float("nan")
    k = (len(values) - 1) * (p / 100.0)
    lo, hi = int(k), min(int(k) + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (k - lo)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--seconds", type=float, default=20.0)
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    samples, errors, lock = [], [0], threading.Lock()
    stop_at = time.monotonic() + args.seconds

    threads = [
        threading.Thread(target=worker, args=(args.url, stop_at, samples, errors, lock))
        for _ in range(args.concurrency)
    ]
    started = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - started

    samples.sort()
    if not samples:
        print(f"{args.label or args.url}: no successful requests ({errors[0]} errors)")
        sys.exit(1)

    print(f"{args.label or args.url}")
    print(f"  requests   {len(samples)} in {elapsed:.1f}s  ({len(samples)/elapsed:.0f} req/s)")
    print(f"  errors     {errors[0]}")
    print(f"  mean       {statistics.fmean(samples):8.2f} ms")
    print(f"  p50        {percentile(samples, 50):8.2f} ms")
    print(f"  p95        {percentile(samples, 95):8.2f} ms")
    print(f"  p99        {percentile(samples, 99):8.2f} ms")
    print(f"  max        {samples[-1]:8.2f} ms")

if __name__ == "__main__":
    main()
