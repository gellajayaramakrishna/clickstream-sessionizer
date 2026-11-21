# clickstream-sessionizer

A Rust pipeline that turns tens of millions of messy click-stream events into two
exact daily Parquet tables: one row per person per day, and one row per campaign
per day. Built for the Databricks "Shopping Sessions" track of the Builders
Buildathon (Ohio State, Nov 2026).

What it handles:

- dirty CSV: random quoting, malformed lines, `\r` as data, three timestamp formats
  with strict validation, bad prices, unknown products and campaigns
- duplicate events (earliest ingest wins, dedup before validation) and late events
- one person with many user IDs, joined through long link chains and shared devices
- 30-minute sessions, last-click attribution within 7 days, and assisting campaigns

## How it is fast

No engine and no JVM, just one native binary.

1. **Parallel `pread` input.** Files are cut into 2 MB pieces; each thread reads its piece
   into a reused buffer and keeps the lines that start inside it. No page-table churn,
   and many reads in flight when the disk is cold.
2. **SIMD CSV scanning.** AVX2 compares find commas, quotes and newlines 64 bytes at a
   time. Only commas and line ends are visited; quotes are validated once per line with
   a counting rule instead of per character.
3. **IDs as integers.** Every ID is packed into a `u64` (6 bits per character plus the
   length) in a way that keeps byte-wise order, so comparing, hashing and naming people
   never touches strings. Packing runs 16 bytes at a time with SSE; odd IDs fall back to
   an interned table.
4. **SWAR timestamps.** `YYYY-MM-DDTHH:MM:SSZ` is validated and converted eight bytes at
   a time, with month lengths from a precomputed table.
5. **Two parallel shuffles.** Events are bucketed by event ID for dedup, then survivors
   and clicks are bucketed by person. Buckets live in one huge-page arena, filled through
   small per-thread staging buffers.
6. **Union-find for linked IDs.** It runs on one thread while the others parse events.
   Person lookups use a flat open-addressing table with batched prefetching.
7. **Cache-friendly sessions.** Each bucket is grouped by person with a counting sort,
   then each person's events are sorted by time and walked once.
8. **Direct Parquet writer.** A small PLAIN, uncompressed writer encodes columns while
   rows are produced.

## Run

```sh
cargo build --release
DATA_DIR=/path/to/data OUTPUT_DIR=/path/to/output ./target/release/clickstream-sessionizer
```

Set `TIMING=1` to print stage timings to stderr. `NCPU` sets the thread count.

Docker (the scorer's way):

```sh
docker build -t shopping-sessions .
docker run --rm --read-only --user 1000:1000 -e NCPU=8 \
  -v "$PWD/dev":/data:ro -v "$PWD/out":/output shopping-sessions
```

## Tests

`cargo test` covers the ID packing order (scalar and SIMD), the CSV dialect, the input
splitting and the timestamp rules, including a 2-million-case fuzz test of the fast parser.
The pipeline passes all 36 edge-case fixtures of the track and matches the published
1M-event dev answer exactly.
