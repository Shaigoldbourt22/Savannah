# Binance Spot Local Order Book Design

## Purpose and scope

This system maintains an in-memory, price-level copy of the Binance Spot order book for one configurable trading pair. A .NET 10 worker combines a REST depth snapshot with live WebSocket updates, checks their update IDs, and rebuilds the book when it cannot establish continuity. It prints the known book before and after the first applied update and reports readiness and processing data.

Binance provides aggregate quantities at each buy and sell price, not individual orders. The worker tracks those levels; it does not match orders or place trades. This version uses one Binance feed and one pair. Additional pairs, feeds, and external access are future work.

## Assumptions and correctness boundary

- The REST snapshot and diff-depth stream refer to the same configured Binance Spot pair. Stream events contain ordered update-ID ranges and give the **new total quantity** at each listed price, not a quantity to add to the old one.
- A snapshot contains at most 5,000 price levels per side. Untouched levels beyond it are unknown; later events may add known levels outside that initial range. The highest known buy and lowest known sell are not guaranteed to be the best prices across Binance's full book. An empty known side does not prove the exchange side is empty.
- `book_ready=1` means the worker joined a snapshot to an applicable event and has not detected a gap or failure since. It does not prove that every exchange level is known or that the copy is current to the instant: network lag and an undetected silent connection can leave it behind. The 100 ms stream setting is an update cadence, not an end-to-end latency guarantee.
- The book is held only in memory. After a restart or detected loss of continuity, it is unavailable as a ready book until a fresh snapshot and stream join succeeds.

## Functional and quality requirements

### Functional requirements

1. Start the diff-depth WebSocket for a configurable Binance Spot pair and buffer events while fetching its REST depth snapshot.
2. Join the snapshot to the buffered stream using update IDs, then apply later events in arrival order. Skip covered events; detect missing updates and rebuild from a fresh stream and snapshot.
3. Keep separate ordered buy and sell levels. Set each listed price to its reported quantity, add unknown prices, and remove prices reported with zero quantity.
4. Validate each event before changing the book. Mark the book not ready during startup or recovery, and ready only after a valid snapshot-to-stream join.
5. Print the known book before and after the first applied update, including the pair, update ID, and buy and sell levels.
6. Report readiness, received and handled events, snapshot use, recovery, and update-processing times.

### Quality goals

- **Timeliness:** Use Binance's 100 ms diff-depth stream. Aim for under 10 ms of local update processing, leaving time for other work, but treat this as a goal to measure rather than a proven bound or a network-latency guarantee.
- **Reliability:** Do not keep a book marked ready after a detected gap, invalid message, disconnect, or buffer overflow. Rebuild it rather than silently discarding needed updates.
- **Resource use and stability:** Limit the queued raw events and retry failed snapshot requests with backoff. Run continuously without requiring a saved copy of the book.
- **Future scale:** Keep the book logic separate from Binance-specific transport and sequencing so later versions can manage independent books for more pairs or sources.

## System architecture

### Components and data flow

One .NET 10 process owns one Binance pair and one in-memory book. It needs outbound access to Binance's market-data WebSocket and REST depth endpoint; it has no inbound API.

```text
Binance diff-depth WebSocket --> receiver --> per-attempt FIFO queue --+
                                                                     +--> sync loop --> validated events --> book
Binance REST depth snapshot -----------------------> snapshot fetch --+                         |
                                                                                                +--> before/after output
                                                                                                +--> readiness and telemetry logs
```

- [Program](./src/Savannah.OrderBook/Program.cs) selects and checks the pair, starts the worker, and handles shutdown.
- [BinanceWorker](./src/Savannah.OrderBook/BinanceWorker.cs) starts a receiver, fetches snapshots, and consumes queued events in order. The same consumer handles buffered and live events. On a failed attempt, it cancels and awaits the receiver; the next attempt gets a new socket and queue.
- [BufferedUpdates](./src/Savannah.OrderBook/BufferedUpdates.cs) holds raw messages in arrival order and rejects a write when the queue exceeds its event or byte limit. [BinanceMessages](./src/Savannah.OrderBook/BinanceMessages.cs) parses and checks message fields; the worker checks update-ID continuity before changing the book.
- [OrderBook](./src/Savannah.OrderBook/OrderBook.cs) stores known buy and sell levels, the last update ID, and readiness. [BookTelemetry](./src/Savannah.OrderBook/BookTelemetry.cs) counts activity and records apply times. Console output includes the first before/after example and periodic readiness and telemetry lines.

### Book representation and data structure trade-offs

Each side maps a `decimal` price to its aggregate `decimal` quantity. Bids use a descending comparer and asks an ascending comparer in separate .NET `SortedDictionary<decimal, decimal>` instances. These use balanced red-black trees. The book keeps update IDs as `long`; price and quantity strings are parsed using invariant culture instead of binary floating-point numbers.

| Structure | Change a price level | Find the best known price | Trade-off |
| --- | --- | --- | --- |
| **SortedDictionary (chosen)** | O(log n) lookup, insert, or removal | Ordered iteration starts at the best level; O(log n) to reach it | Predictable changes and ordered output, with a node per level. |
| Hash map alone | Average O(1) lookup and change | O(n) scan | Fast direct updates but no price ordering. |
| Sorted list | O(log n) search; O(n) insert or removal | O(1) at the end | Compact storage, but moving entries can make large bursts costly. |

For `m` changed levels and `n` known levels on a side, an event takes O(m log n) to apply and the book uses O(n) storage. No separate hash map is needed. Each quantity is **replaced**, not added; zero removes the level. The book validates both sides before applying an event so invalid data cannot partly change it.

## Binance snapshot and stream synchronization

Each diff-depth event has a first update ID `U` and final update ID `u`. The snapshot has `lastUpdateId`; call the book's current update ID `L`.

### Starting the book

1. Open the configured pair's `@depth@100ms` WebSocket. Queue complete messages in arrival order and record `U` from the first event.
2. While the receiver keeps queuing messages, request `GET /api/v3/depth?symbol=<PAIR>&limit=5000`. If `snapshot.lastUpdateId < first U`, request another snapshot without stopping the receiver.
3. Load the snapshot's buy and sell levels with `L = lastUpdateId`; the book remains not ready. Ignore queued events with `u <= L`. If no newer event is queued, wait for one.
4. The first applicable event must cover the next ID: `U <= L + 1 <= u`. Apply it, mark the book ready, and continue consuming the **same queue** for live events. If its `U > L + 1`, discard this attempt and rebuild.

### Applying depth updates

For each subsequent event, parse and validate it, then compare its ID range with `L`:

- If `u <= L`, it is already covered; skip it.
- If `U > L + 1`, updates are missing; invalidate the book and start a new sync attempt.
- Otherwise, apply it even if its range overlaps an earlier event (`U <= L + 1 <= u`).

For every listed buy (`b`) and sell (`a`) price, set the quantity to the reported value. A zero quantity removes the level, including when the level is already absent. Unlisted prices do not change. After applying the event, set `L = u`.

### Validating data and detecting gaps

The snapshot must have a positive update ID and valid buy/sell price arrays. It has no symbol field, so the request uses the configured pair; every WebSocket event must carry that pair and have type `depthUpdate`. Events require positive IDs with `U <= u`, two-string price/quantity entries, positive prices, and nonnegative quantities. Prices and quantities must parse as `decimal` with invariant culture. The full event is checked before its price levels are changed.

An invalid payload or missed ID invalidates the attempt instead of applying a partial or out-of-sequence update. A new connection, queue, and snapshot are used for recovery; retry details are in the next section.

## Failures and recovery

When continuity or input validity fails, the worker marks the book not ready and logs the cause. It cancels and closes the old WebSocket, awaits its receiver, discards that attempt's queue, and starts with a new connection and snapshot. Events from the old attempt cannot join the new snapshot. There is no downstream API to serve a stale book.

| Failure | Response |
| --- | --- |
| Missing update ID, invalid event or snapshot, WebSocket close/error | Invalidate the book and rebuild from a new stream and snapshot. Do not apply later queued events from the failed attempt. |
| Snapshot ID older than the first event's `U` | Fetch another snapshot while the receiver continues buffering; keep the book not ready. |
| More than 2,000 queued events, more than 16 MiB of queued raw payload, or a WebSocket message over 16 MiB | Fail the attempt and reconnect instead of dropping individual events. |
| REST timeout or transient network/HTTP error | Allow 5 seconds per snapshot request, then rebuild after jittered exponential backoff. The nominal delay grows from 1 to 30 seconds; jitter can make the actual delay exceed 30 seconds. |
| REST `429` or `418` with a valid `Retry-After` | Close the stream, discard its queue, wait the specified number of seconds, then start a new attempt. |
| REST `429` or `418` without a valid `Retry-After` | Stop the worker with an error instead of guessing a cooldown. |

Successful synchronization resets the transient-failure backoff. A silent connection that neither delivers data nor reports a close/error is **not** detected by the current worker; no-update periods alone also do not prove a fault. This limits how strongly readiness can be interpreted.

## Persistence and durability trade-offs

The book, its last update ID, and the pending-event queue live only in process memory. No book snapshot or update history is written to a database. A restart loses that state: the worker opens a new stream, fetches a fresh Binance snapshot, and stays not ready until an event joins them. Recovery therefore causes a period without a ready book; old queued updates cannot be replayed after a restart.

This is a deliberate trade-off for a read-only copy of Binance data. In-memory updates avoid a remote write for every changed price and keep deployment simple. Redis, disk storage, or a durable queue could retain a copy across restarts, but would add cost and operational work. Such a copy could still be stale after downtime, so persistence alone would not remove the need to check continuity against Binance or rebuild. Console logs support diagnosis, not book recovery.

## Performance and resource limits

Binance's `@depth@100ms` option sets the stream's update cadence, not a delivery deadline or maximum event size. The design goal is to keep **local update processing** below 10 ms so other work has time within a 100 ms interval; this is not a measured guarantee for every event. A burst of 5,000 price-level changes per pair in 100 ms is a hypothetical stress scenario, **not** a documented Binance maximum. Tree operations take O(log n) per changed level; event cost also includes parsing, validation, and queue wait.

The worker records the time spent in `OrderBook.Apply` for each applied event. Every 30 seconds it reports average and median apply time from at most the latest 10,000 samples in that interval. A separate receive-to-apply delay, which includes queue wait and parsing but not network transit before receipt, is logged every 100 applied events. The synthetic stress check times 100 batches of 2,000 price-level changes and reports p95 and p99 against the 10 ms design goal; it does not test the entire receive-to-apply path or the unverified 5,000-change scenario.

The pending-event queue rejects writes beyond **2,000 messages or 16 MiB of raw payload**, whichever comes first; a single WebSocket message over 16 MiB is also rejected. Overflow triggers a rebuild rather than losing an event silently. These are safety limits, not total process-memory limits: JSON parsing, tree nodes, copies of messages, and telemetry samples use more memory. The initial snapshot has at most 5,000 levels per side, but later events can add more; the book has no fixed level cap. Peak arrival rate, queue growth, CPU, memory, and p95/p99 end-to-end delays still need measurement under representative live and burst loads.

## Deployment

The [Dockerfile](./Dockerfile) publishes the .NET 10 worker in an SDK build stage, then runs it as a non-root user on the smaller .NET runtime image. The pair comes from a command-line argument or the `SYMBOL` environment variable (`BNBBTC` by default). The worker needs outbound WebSocket and HTTPS access to Binance, but no trading credentials or inbound network access.

| Environment | Container App | Resource group | Log Analytics workspace |
| --- | --- | --- | --- |
| Staging | `app-savannah-eu-stage` | `rg-savannah-staging` | `log-savannah-staging` |
| Production | `app-savannah-eu-prod` | `rg-savannah-prod` | `log-savannah-prod` |

Both Container Apps run in North Europe in separate Container Apps environments. Each has ingress disabled and a minimum and maximum of one replica for its configured pair. Each environment uses its own container registry and a user-assigned managed identity to pull its image. Deploying by image digest avoids a tag changing underneath a running revision; the current staging and production apps reference the same image digest. Console output is collected in the corresponding Log Analytics workspace.

Azure Container Apps provides container hosting, rollout, and logging without maintaining VM operating systems. One VM per pair would add host-management work; one shared VM would tie all pairs to one host. The single-replica setting also avoids deliberate duplicate workers for the same pair within an app. During restarts or rollouts each in-memory book must synchronize again, and overlapping revisions would not share state.

Staging also has separate scheduled Container Apps jobs for daily live checks and weekly synthetic stress checks. Their test image is built from [Dockerfile.staging-tests](./Dockerfile.staging-tests); they are not the always-running book worker. The selected region must allow Binance market-data access.

## Scalability and future extensions

The current process accepts **one pair and one Binance feed**. To add pairs, assign disjoint pairs or pair groups to separate workers and Container Apps. Each `(source, pair)` needs its own book, update ID, receiver, queue, snapshot join, retry state, and readiness signal. A multi-pair worker would need a separate sync loop for each pair; it is not implemented here.

Horizontal growth should add apps with different pair assignments, not extra replicas consuming the same pair. Pair ownership must also account for overlapping revisions during rollout: without coordination, two workers could both claim to be the authoritative copy. Group sizes and replica resources should be chosen from measured arrival rates, CPU, memory, backlog, and Binance's shared connection and REST rate limits rather than an assumed number of pairs per host.

For another exchange, add a source-specific adapter for its transport, snapshot format, and continuity rules. The price-level book can be reused for normalized price/quantity changes, but each source keeps a separate book and its own update IDs; prices from different exchanges must not be mixed. Today [BinanceWorker](./src/Savannah.OrderBook/BinanceWorker.cs) contains the Binance connection and sync logic, so this adapter split is a future change, not an existing plug-in interface.

## Out-of-scope features

This version does not include:

- Multiple pairs in one worker, another exchange feed, or a consolidated cross-exchange book.
- Individual order tracking, matching, trade execution, or account data. Binance supplies aggregate price levels, not an order-by-order feed for this worker.
- A user interface or inbound API, including an endpoint for the best known buy or sell price. Output is a bounded console view.
- Durable book storage or replaying a saved book after restart. Recovery uses a new Binance snapshot and stream instead.
- A guarantee of complete exchange depth or a best price outside the levels known from the snapshot and subsequent updates.

## Observability and monitoring plan

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

## Testing and verification plan

Verification separates repeatable offline checks from live Binance checks. A passing build or a ready heartbeat alone does not prove that every price level is correct.

### Offline correctness checks

The [CI workflow](./.github/workflows/ci.yml) restores and builds with warnings as errors on pushes and pull requests, then runs deterministic tests while excluding the `Stress` and `Staging` categories. Existing tests cover the assignment's snapshot/update values, bid/ask ordering, replacement and zero removal, stale/overlapping/gapped IDs, invalid payloads without partial book changes, queue order and overflow, rate-limit headers, readiness sampling, and telemetry calculations.

The current snapshot-fetch check starts with an event already buffered; it does **not** exercise a delayed REST request with concurrent WebSocket events or prove the full worker rebuilds correctly. The next offline integration check should use controlled HTTP and WebSocket inputs to verify the snapshot join, the printed before/after example, a too-old snapshot, gaps, disconnections, invalid events, overflow, `Retry-After` delays, and fresh attempt isolation. For each failure, assert that readiness drops and stays down until a new valid join.

### Live and staging checks

Staging runs separate scheduled jobs: the daily check connects to Binance, waits for a ready book, and checks that its update ID advances; the weekly check runs synthetic one-pair bursts. These checks do not depend on production traffic. Restart the staging worker and verify in its logs that it starts not ready, loads a new snapshot, joins the stream, and returns to ready. Check for repeated failures or a growing backlog during a longer run.

A saved snapshot-and-event replay through the **whole worker**, compared against a complete expected book, is still needed; the current daily live check only proves a join and subsequent progress. Before promoting a release, run the offline checks and the live, stress, and restart checks against the candidate version in staging. Ensure the test image and deployed worker use the same candidate source; this release gate is a plan, not an automated CI gate today.

### Performance measurements

The existing stress check starts with 5,000 levels per side and times 100 batches of 2,000 price changes. It reports p95, p99, and a memory delta; host-dependent timing is not treated as a correctness test failure. It times book operations rather than JSON parsing, queueing, or network delivery and does not validate the hypothetical 5,000-change burst.

Measure live messages and changed levels per 100 ms, apply-only and receive-to-apply p95/p99, queue growth, process memory, and recovery frequency. Replay larger bursts with known final books and compare the ordered tree with a sorted list before making claims about peak throughput or machine capacity.

## Before-and-after order book example

The assignment's numeric example starts with snapshot ID `157`: buy price `4` has quantity `431`, and sell price `4.000002` has quantity `12`. A diff-depth event with `U=158` and `u=160` sets buy `0.0024` to `10`, removes buy `4` with quantity `0`, and sets sell `0.0026` to `100`. Since `158 = 157 + 1`, the event joins the snapshot.

With these values, the worker's before/after console format is:

```text
BEFORE UPDATE
ORDER BOOK BNBBTC | lastUpdateId=157 | known levels (bids=1, asks=1)
BIDS:
4 : 431
ASKS:
4.000002 : 12
AFTER UPDATE
ORDER BOOK BNBBTC | lastUpdateId=160 | known levels (bids=1, asks=2)
BIDS:
0.0024 : 10
ASKS:
0.0026 : 100
4.000002 : 12
```

The old buy level is gone; the original sell level remains because the event did not change it. The worker shows at most five known levels per side, while the counts include all known levels. This is a repeatable sample, not a live exchange capture.

A separate [local before-and-after print](./orderbook-before-after.md) captures a real BNBBTC snapshot and applied update from Binance.

## References

- [Binance: managing a local order book](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#how-to-manage-a-local-order-book-correctly)
- [Binance: diff-depth stream format](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#diff-depth-stream)
- [Binance: REST order book snapshot](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#order-book)
- [.NET: SortedDictionary](https://learn.microsoft.com/en-us/dotnet/api/system.collections.generic.sorteddictionary-2?view=net-10.0)
- [Azure Container Apps overview](https://learn.microsoft.com/en-us/azure/container-apps/overview)
- [Azure Container Apps logging](https://learn.microsoft.com/en-us/azure/container-apps/log-monitoring)
