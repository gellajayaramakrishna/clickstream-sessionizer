# Shopping Sessions pipeline (Rust)

Single native binary, multithreaded, reads the CSVs with mmap and writes both Parquet tables.

Phases:
1. Parse events and clicks in parallel 4 MB chunks (one thread builds the linked-ID union-find at the same time). Events are scattered into buckets by event_id hash.
2. Per bucket: dedup on event_id (smallest ingest_ts wins, then validity check), map every user ID to its person, scatter by person.
3. Per person bucket: sort by (person, ts), build sessions, credit the latest click within 7 days, count assists with merged per-campaign intervals, aggregate per day.

Build and run:

    docker build -t shopping-sessions .
    docker run --rm -v $PWD/dev:/data:ro -v $PWD/out:/output shopping-sessions
