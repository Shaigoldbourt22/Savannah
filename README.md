# Savannah order book

A one-pair Binance Spot price-level book. It connects to Binance's market-data-only diff-depth WebSocket and snapshot API, joins a REST snapshot with buffered updates, then keeps the known price levels in memory. It does not place trades or claim to represent price levels outside Binance's snapshot and received changes.

## Run

Requires the .NET 10 SDK and network access to Binance.

```powershell
dotnet run --project src\Savannah.OrderBook -- BNBBTC
```

The pair can also be set with the `SYMBOL` environment variable. Ctrl+C stops the worker. It prints the book before and after the first applicable update, then periodic status lines. A zero quantity removes a price; an existing quantity is replaced, not added.

## Build and check

```powershell
dotnet restore Savannah.sln
dotnet build Savannah.sln --no-restore -warnaserror
dotnet test Savannah.sln --no-restore
```

CI uses the same build and offline tests, excluding `Stress` and `Staging` categories. Live Binance availability is not required for CI.

## Container

```powershell
docker build -t savannah-orderbook:local .
docker run --rm -e SYMBOL=BNBBTC savannah-orderbook:local
```

Deploy as one always-running Container App with ingress disabled and a single replica for the pair. Set `SYMBOL` to the desired Binance Spot pair. On a disconnect, gap, or restart the book is marked not ready, then rebuilt from a new stream and snapshot. The process keeps no durable copy of the book.

Staging uses a separate test container (`Dockerfile.staging-tests`): the `Stress` category replays synthetic bursts for one pair, and `Staging` checks a live join and subsequent updates. Keep these runs separate from the always-running worker and CI. Use a region where Binance's public market-data endpoints are available.

The staging worker is `app-savannah-eu-stage` in the `aca-savannah-eu-staging` environment, resource group `rg-savannah-staging` (North Europe). It has no ingress and runs one replica. Logs go to `log-savannah-staging`. Two scheduled Container Apps jobs run at 06:00 UTC: `job-savannah-daily` checks live data every day, and `job-savannah-weekly` runs one-pair stress every Sunday. To rerun them manually:

```powershell
az containerapp job start -g rg-savannah-staging -n job-savannah-daily
az containerapp job start -g rg-savannah-staging -n job-savannah-weekly
az containerapp job execution list -g rg-savannah-staging -n job-savannah-daily
az containerapp logs show -g rg-savannah-staging -n app-savannah-eu-stage --tail 50
```

Binance returned HTTP 451 from East US 2. Staging runs in North Europe; check availability before changing its region.

## Design limits

Binance REST snapshots include at most 5,000 levels on each side. The best known price may not be the best price across the full exchange when deeper levels are not known. The initial event buffer limit is 2,000 messages or 16 MiB of raw payload. Missing update IDs or connection loss cause a full rebuild; the worker never uses a stale book as current.
