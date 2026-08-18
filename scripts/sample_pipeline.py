#!/usr/bin/env python3
"""Poll /api/status once a second and print queue depths and throughput.

Used to show backpressure taking hold when the database writer is paused.

  ./scripts/sample_pipeline.py --seconds 60 > backpressure.tsv
"""

import argparse
import json
import sys
import time
import urllib.request

FIELDS = [
    "elapsed_s", "fetch_queue", "write_queue", "slots_done", "slots_skipped",
    "blocks_written", "write_batches", "writer_paused", "transactions", "trades",
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080/api/status")
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--interval", type=float, default=1.0)
    args = ap.parse_args()

    print("\t".join(["t", *FIELDS, "slots_per_s", "blocks_per_s"]))
    stop_at = time.monotonic() + args.seconds
    t0 = time.monotonic()
    prev = None

    while time.monotonic() < stop_at:
        try:
            with urllib.request.urlopen(args.url, timeout=10) as r:
                s = json.load(r)
        except Exception as e:  # noqa: BLE001 - the server may still be starting
            print(f"# {e}", file=sys.stderr)
            time.sleep(args.interval)
            continue

        now = time.monotonic() - t0
        settled = s["slots_done"] + s["slots_skipped"]
        rates = ["", ""]
        if prev:
            dt = now - prev[0]
            if dt > 0:
                rates = [
                    f"{(settled - prev[1]) / dt:.2f}",
                    f"{(s['blocks_written'] - prev[2]) / dt:.2f}",
                ]
        prev = (now, settled, s["blocks_written"])

        print("\t".join([f"{now:.1f}", *(str(s[f]) for f in FIELDS), *rates]), flush=True)

        if s["ingest_done"]:
            break
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
