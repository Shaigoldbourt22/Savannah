# Testing and monitoring plan

This plan covers the one-pair Binance Spot price-level worker described in [Design.md](./Design.md). It separates repeatable offline checks from live data checks. A passing build or a ready heartbeat alone does not prove every known price level is correct.

## Monitoring

The worker writes structured text to standard output and errors to standard error; Azure collects both in each environment's Log Analytics workspace. These are **log-derived signals**, not exported Azure custom metrics. Readiness transitions, connection and snapshot activity, and rebuild causes are logged without printing every price change.

| Signal | What it shows |
| --- | --- |
| `readiness ... book_ready=0|1` | Sampled every 30 seconds, including quiet periods, with the pair and last update ID. Readiness changes are also logged immediately. |
| `apply_avg_ms`, `apply_median_ms`, `apply_samples` | Every 30 seconds, average and median time inside `OrderBook.Apply` for at most the latest 10,000 applied messages in that interval; `n/a` if none. This excludes parsing and time waiting in the queue. |
| `update_processing_delay_ms`, `buffered_updates` | Logged every 100 applied messages. Delay runs from local receipt to application, including queue wait and parsing; backlog shows whether the worker is falling behind. |
| `received_total`, `applied_total`, `skipped_total` | Cumulative messages received by this process, applied, or correctly skipped as already covered. `handled_pct_received = 100 * (applied + skipped) / received`; `applied_pct_received = 100 * applied / received`. Both are `n/a` if none were received. |
| `snapshot_requests_total`, `snapshots_loaded_total`, `snapshot_failures_total`, `resync_total` | Cumulative attempts, snapshots actually loaded, failures, and rebuilds. Differences between reports show how often snapshots are loaded over a chosen window. |

To estimate **book-ready time over 10 minutes**, divide the window into 20 thirty-second slots and count at most one readiness sample per slot: `100 * ready slots / 20`. Count a missing slot as unavailable, not as ready. This is an estimate: a short outage between samples can be missed, ingestion can lag, and a silent but undetected connection can still report ready. Immediate transition logs help investigate those gaps.

The message percentages use only events this worker received; they cannot measure events Binance sent while it was disconnected. Skipping an event already covered by a snapshot is correct handling, not a failed update. The logged per-interval medians cannot be combined into an exact ten-minute median.

**Example from live Azure Container Apps logs.** These BNBBTC readings came from the staging and production workers on September 27, 2026. Apply times summarize the preceding 30-second reporting interval; snapshots loaded and failures are cumulative since each worker started. The values are observations, not a latency or availability guarantee.

| Environment | Book ready | Apply average (ms) | Apply median (ms) | Updates handled (%) | Snapshots loaded | Snapshot failures | Last report (UTC) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Staging | 1 | 0.010 | 0.007 | 100.00 | 1 | 0 | 2026-09-27 15:16:36.690 |
| Production | 1 | 0.008 | 0.006 | 100.00 | 1 | 0 | 2026-09-27 15:16:44.330 |

Suggested alerts are sustained `book_ready=0`, missing heartbeats, repeated resyncs or snapshot failures, rate limits, growing backlog, and high processing delay. **Automated alert rules are not configured**; silence without other evidence does not by itself prove a feed failure.

## Testing and verification

### Offline correctness checks

The [CI workflow](./.github/workflows/ci.yml) restores and builds with warnings as errors on pushes and pull requests, then runs deterministic tests while excluding the `Stress` and `Staging` categories. Existing tests cover the assignment's snapshot/update values, bid/ask ordering, replacement and zero removal, stale/overlapping/gapped IDs, invalid payloads without partial book changes, queue order and overflow, rate-limit headers, readiness sampling, and telemetry calculations.

The current snapshot-fetch check starts with an event already buffered; it does **not** exercise a delayed REST request with concurrent WebSocket events or prove the full worker rebuilds correctly. The next offline integration check should use controlled HTTP and WebSocket inputs to verify the snapshot join, the printed before/after example, a too-old snapshot, gaps, disconnections, invalid events, overflow, `Retry-After` delays, and fresh attempt isolation. For each failure, assert that readiness drops and stays down until a new valid join.

### Live and staging checks

Staging runs separate scheduled jobs: the daily check connects to Binance, waits for a ready book, and checks that its update ID advances; the weekly check runs synthetic one-pair bursts. These checks do not depend on production traffic. Restart the staging worker and verify in its logs that it starts not ready, loads a new snapshot, joins the stream, and returns to ready. Check for repeated failures or a growing backlog during a longer run.

A saved snapshot-and-event replay through the **whole worker**, compared against a complete expected book, is still needed; the current daily live check only proves a join and subsequent progress. Before promoting a release, run the offline checks and the live, stress, and restart checks against the candidate version in staging. Ensure the test image and deployed worker use the same candidate source; this release gate is a plan, not an automated CI gate today.

### Performance measurements

The existing stress check starts with 5,000 levels per side and times 100 batches of 2,000 price changes. It reports p95, p99, and a memory delta; host-dependent timing is not treated as a correctness test failure. It times book operations rather than JSON parsing, queueing, or network delivery and does not validate the hypothetical 5,000-change burst.

Measure live messages and changed levels per 100 ms, apply-only and receive-to-apply p95/p99, queue growth, process memory, and recovery frequency. Replay larger bursts with known final books and compare the ordered tree with a sorted list before making claims about peak throughput or machine capacity.
