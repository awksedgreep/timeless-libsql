#!/usr/bin/env python3
"""Exercise configured data-time expiration through the public HTTP/SQL path."""

import argparse
import json
import sys

sys.dont_write_bytecode = True
import ingest  # noqa: E402
import maintenance  # noqa: E402


def storage_shape(stats):
    return {
        key: stats[key]
        for key in (
            "total_spans",
            "blocks",
            "raw_blocks",
            "bytes_on_disk",
            "oldest_timestamp_nanoseconds",
            "newest_timestamp_nanoseconds",
            "database_file_bytes",
            "database_wal_bytes",
            "sqlite_page_bytes",
            "freelist_pages",
            "freelist_bytes",
            "physical_database_bytes",
        )
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:19449")
    parser.add_argument("--batch", type=int, default=8192)
    args = parser.parse_args()
    base = args.url.rstrip("/")
    epoch_one = 1_700_002_000_000_000_000
    epoch_two = epoch_one + 2_000_000_000

    maintenance.post(base, ingest.body(3_000, 0, args.batch, epoch_one))
    ingest.request_json(base + "/api/v1/flush", "POST")
    first = ingest.request_json(base + "/select/traces/stats")

    maintenance.post(base, ingest.body(3_001, 0, args.batch, epoch_two))
    flush = ingest.request_json(base + "/api/v1/flush", "POST")
    second = ingest.request_json(base + "/select/traces/stats")
    report = {
        "retention_nanoseconds": second["retention_nanoseconds"],
        "epoch_one": storage_shape(first),
        "epoch_two_after_automatic_expiration": storage_shape(second),
        "flush": flush,
        "queued_requests": second["queued_requests"],
        "in_flight_requests": second["in_flight_requests"],
        "failed_spans": second["failed_spans"],
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if first["total_spans"] != args.batch:
        raise SystemExit("first epoch was not durable")
    if second["total_spans"] != args.batch:
        raise SystemExit("automatic retention did not leave exactly the new epoch")
    if second["oldest_timestamp_nanoseconds"] < epoch_two:
        raise SystemExit("expired epoch remains queryable")
    if second["queued_requests"] or second["in_flight_requests"]:
        raise SystemExit("retention run did not drain")


if __name__ == "__main__":
    main()
