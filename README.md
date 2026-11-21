# Shopping Sessions: Rust pipeline

A single statically scheduled, multithreaded Rust binary that implements
`rules.md` exactly: it reads the CSVs, links IDs into people, dedups, validates
and sessionizes events, credits and assists campaigns, and writes both Parquet
tables. No engine, no runtime dependencies. The 28 MB image starts instantly.

## Pipeline

Every stage is a linear pass, parallel over `$NCPU` worker threads that pull
work items from an atomic counter. Data moves between stages through
hash-partitioned buffers (P = 512 partitions per thread), so no stage needs a
global lock or a global sort.

| Stage | Work | Cost |
| --- | --- | --- |
| Links | Parallel parse + 128-bit hash of each ID; parallel interning by hash region; union-find (`parent[x] <= x`, so one ascending pass flattens it); smallest ID per person; region-partitioned open-addressing lookup table | O(L) |
| 1. Parse | mmap'd inputs split into ~2 MB line-aligned chunks. A SIMD scanner (AVX2 on x86, NEON on ARM) finds `,` `"` `\n` 64 bytes at a time. Each valid event is resolved to its person (prefetched table lookup) and written once into its **person** partition. Every parsed line also leaves a 32-byte dedup record in its **event_id** partition. Click chunks share the same work queue | O(E + C) |
| 2. Dedup | Per event_id partition: an open-addressing table keeps the copy with the smallest `ingest_ts` (invalid copies compete too, so dedup comes before validation). Losing copies are flagged in per-partition bitmaps (only ~3% of events) | O(E) |
| 3. People | Per person partition (largest first): group by 128-bit person hash, counting-sort events and clicks by person, sort each person's events by `ts`, cut sessions at gaps > 30 min, aggregate days and top category, credit each session with a two-pointer scan over the person's `(ts, click_id)`-sorted clicks, and count assists as a union of session ranges per campaign | O(E log k), k = events per person |
| 4. Write | A minimal hand-written Parquet writer (PLAIN, uncompressed, one page per column), one `user_daily` file per thread written in parallel | O(rows) |

The only super-linear part is sorting each person's own events, which is
small per person.

Exactness details:
- Every dialect rule (quotes only around whole fields, `\r` as data, ASCII-only digits, F1/F2/F3 timestamps with calendar and offset checks, F3 overflow) is in `src/parse.rs` with unit tests and a randomized comparison against a naive splitter.
- IDs, event_ids and click_ids are compared byte-exactly. The link table and person grouping use 128-bit xxh3 hashes as identity; at 128 bits a collision is not a practical concern.

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
- `examples/scan.rs`: CSV scanner micro-benchmark (`cargo run --release --example scan -- DIR`)
