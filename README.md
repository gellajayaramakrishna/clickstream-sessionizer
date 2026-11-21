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

No engine and no JVM, just one native binary. Every event is read once and moved once.

1. **Linked IDs first, on all cores.** Link files are parsed in parallel and joined with a
   lock-free union-find (compare-and-swap unions, path halving). The result is a flat
   table from each linked ID to its person, fronted by a small Bloom filter so that IDs
   with no links are answered without touching the table.
2. **Mapped input, populated per piece.** Files are cut into 2 MB pieces; each thread asks
   the kernel to map its piece in one call (`MADV_POPULATE_READ`) and keeps the lines that
   start inside it. `pread` is the fallback.
3. **SIMD CSV scanning.** AVX2 compares find commas, quotes and newlines 64 bytes at a
   time. Only commas and line ends are visited; quotes are validated once per line with a
   counting rule.
4. **IDs as integers.** Every ID is packed into a `u64` (6 bits per character plus the
   length) so that integer order is byte order. Packing runs 16 bytes at a time with SSE;
   odd IDs fall back to an interned table. Timestamps are parsed eight bytes at a time.
5. **One pass, two 16-byte records.** Each valid event writes a person record (person,
   time, price and category packed into 32 bits) straight into its person's bucket, and
   every event line writes an event-ID record holding the person record's slot. Records go
   into blocks bump-allocated from one huge-page arena, so memory is used densely.
6. **Dedup by marking.** Each event-ID bucket is checked with a small in-cache hash table;
   the losing copies of a duplicated event (or all copies, if the earliest one is invalid)
   are marked dead in a bitmap. Nothing is moved.
7. **Cache-friendly sessions.** Person buckets are processed largest first. Live records
   are radix-partitioned into 4096 small parts by more hash bits, each part is sorted by
   (person, time) inside L1, then walked once for sessions, last-click credit and assists.
8. **Direct Parquet writer.** A small PLAIN, uncompressed writer encodes columns while rows
   are produced.

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
splitting, the timestamp rules (with fuzz tests of the fast parsers) and the parallel
union-find against a sequential one on random link graphs.
The pipeline passes all 36 edge-case fixtures of the track and matches the published
1M-event dev answer exactly.
