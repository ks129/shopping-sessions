# Shopping Sessions: Rust pipeline

A single statically scheduled, multithreaded Rust binary that implements
`rules.md` exactly: it reads the CSVs, links IDs into people, dedups, validates
and sessionizes events, credits and assists campaigns, and writes both Parquet
tables. No engine, no runtime dependencies. The 28 MB image starts instantly.

## Pipeline

Every stage is a linear pass, parallel over `$NCPU` worker threads that pull
work items from an atomic counter. Records move between stages through
hash-partitioned blocks carved from big huge-page arenas, so no stage needs a
global lock or a global sort.

| Stage | Work | Cost |
| --- | --- | --- |
| Links | Parallel parse + 128-bit hash of each ID; parallel interning by hash region; lock-free parallel union-find (`parent[x] <= x`, so one ascending pass flattens it); smallest ID per person by a lock-free minimum; a bucketed lookup table (16 tags per cache line, entries apart) | O(L) |
| 1. Parse | mmap'd inputs split into ~1 MB line-aligned chunks. A SIMD pass finds `,` `"` `\n` 64 bytes at a time and records comma and newline positions; each line's fields come straight from those, with branch-free quote stripping. SWAR timestamp parsing with a month table. Each valid event is resolved to its person through a three-stage prefetch pipeline and written once (24 bytes) into its **person** partition; every parsed line also leaves a 16-byte dedup record in its **event_id** partition. Records are staged per partition and written as whole cache lines with non-temporal stores. Click chunks share the same work queue | O(E + C) |
| 2. Dedup | Per event_id partition (2048 of them, so each table fits in L2): keep the copy with the smallest `ingest_ts` (invalid copies compete too, so dedup comes before validation); losers are flagged in a bitmap over the event arena | O(E) |
| 3. People | Per person partition (largest first): group by person hash, radix-sort the partition's events by `ts` and scatter them stably by person (so each person's events come out sorted), the same for clicks (ties by `click_id`), cut sessions at gaps > 30 min, aggregate days and top category, credit each session with a two-pointer scan, and count assists with a two-pointer sweep merging session ranges per campaign | O(E + C) |
| 4. Write | A minimal hand-written Parquet writer with dictionary encoding (one dictionary and one bit-packed data page per column), one `user_daily` file per worker, written as soon as that worker runs out of partitions | O(rows) |

The output is about 24 MB at 20M events (dictionary encoding: 14 days,
small counts, about 20 categories, and each person's rows together). That
matters because the container's teardown waits for the output to reach disk.

Exactness details:
- Every dialect rule (quotes only around whole fields, `\r` as data, ASCII-only digits, F1/F2/F3 timestamps with calendar and offset checks, F3 overflow) is in `src/parse.rs` with unit tests and randomized comparisons against a naive splitter and a plain timestamp parser.
- IDs and click_ids are compared byte-exactly where it matters (click ties); linked IDs are identified by 128-bit xxh3 hashes, persons by a 64-bit hash and event_ids by about 75 hash bits, which puts the chance of any wrong merge around one in 10^8 per run.

## Testing

```sh
make test               # unit tests (parser, timestamps, fuzzed CSV dialect)
make native-fixtures    # all 36 fixtures against the native binary
make native-dev         # 1M dev set vs dev-answer (needs dev/ and dev-answer/)
make fixtures           # same fixtures through the linux/amd64 image with scorer flags
```

`scripts/reference.py` is an independent, deliberately literal Python
implementation of `rules.md`. It matches `dev-answer` exactly and is used to
cross-check the Rust output on other generator seeds:

```sh
generate --seed 5 --events 600000 --clicks 200000 --output /tmp/s5
target/release/pipeline /tmp/s5 /tmp/s5-rs
python3 scripts/reference.py /tmp/s5 /tmp/s5-py
python3 scripts/compare.py /tmp/s5-rs /tmp/s5-py
```

## Layout

- `src/main.rs`: the pipeline (links, stages, aggregation)
- `src/parse.rs`: CSV dialect, timestamps, prices, SIMD scanner
- `src/output.rs`: minimal Parquet writer
- `scripts/`: fixture runner, comparator, Docker runner, reference implementation
- `examples/scan.rs`, `examples/lineparse.rs`: scanner and per-line parsing micro-benchmarks (`cargo run --release --example lineparse -- EVENTS_DIR`)
