using System.Diagnostics;
using Savannah.OrderBook;
using Xunit.Abstractions;

namespace Savannah.OrderBook.Tests;

public sealed class StagingChecks(ITestOutputHelper output)
{
    [Fact]
    [Trait("Category", "Stress")]
    public void OnePairBurstKeepsCorrectLevels()
    {
        var book = new OrderBook();
        var bids = Enumerable.Range(1, 5_000)
            .Select(price => new PriceLevel(price, 1m)).ToArray();
        var asks = Enumerable.Range(1, 5_000)
            .Select(price => new PriceLevel(10_000 + price, 1m)).ToArray();
        book.Load(new DepthSnapshot(100, bids, asks));
        var times = new List<double>();
        var startMemory = GC.GetTotalMemory(true);

        for (var batch = 0; batch < 100; batch++)
        {
            var id = 101 + batch;
            var changes = Enumerable.Range(1, 1_000).Select(price =>
                new PriceLevel(price, (batch & 1) == 0 ? 0m : batch + 1m)).ToArray();
            var update = new DepthEvent("BNBBTC", id, id, changes, changes.Select(level =>
                new PriceLevel(10_000m + level.Price, level.Quantity)).ToArray());
            var start = Stopwatch.GetTimestamp();
            Assert.True(UpdateSequence.ShouldApply(book.LastUpdateId, update));
            book.Apply(update);
            times.Add(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
        }

        times.Sort();
        var p95 = times[(int)(times.Count * 0.95) - 1];
        var p99 = times[(int)(times.Count * 0.99) - 1];
        var memoryDelta = GC.GetTotalMemory(true) - startMemory;
        output.WriteLine($"100 x 2,000 price changes: p95={p95:F3}ms p99={p99:F3}ms memory change={memoryDelta} bytes");
        Assert.Equal(200, book.LastUpdateId);
        Assert.Equal(5_000, book.BidCount);
        Assert.Equal(5_000, book.AskCount);
        Assert.True(book.TryGetBid(1, out var quantity));
        Assert.Equal(100m, quantity);
        Assert.True(p95 < 10, $"Proposed 10 ms target was exceeded: p95={p95:F3} ms.");
    }

    [Fact]
    [Trait("Category", "Staging")]
    public async Task LiveBinanceWorkerBecomesReadyAndAdvances()
    {
        using var http = new HttpClient();
        using var stopping = new CancellationTokenSource(TimeSpan.FromSeconds(40));
        var worker = new BinanceWorker(http, "BNBBTC");
        var running = worker.RunAsync(stopping.Token);

        try
        {
            var timer = Stopwatch.StartNew();
            while (!worker.Book.IsReady && timer.Elapsed < TimeSpan.FromSeconds(25))
            {
                if (running.IsCompleted)
                    await running;
                await Task.Delay(100, stopping.Token);
            }

            Assert.True(worker.Book.IsReady, "Book did not become ready from the Binance stream.");
            var firstId = worker.Book.LastUpdateId;
            while (worker.Book.LastUpdateId <= firstId && timer.Elapsed < TimeSpan.FromSeconds(35))
                await Task.Delay(100, stopping.Token);
            Assert.True(worker.Book.LastUpdateId > firstId, "No live updates were applied after joining.");
            output.WriteLine($"Live book advanced from {firstId} to {worker.Book.LastUpdateId}");
        }
        finally
        {
            stopping.Cancel();
            await running;
        }
    }
}
