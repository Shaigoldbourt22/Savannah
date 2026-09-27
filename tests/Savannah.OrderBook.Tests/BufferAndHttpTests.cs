using System.Net;
using System.Text;
using System.Threading.Channels;
using Savannah.OrderBook;

namespace Savannah.OrderBook.Tests;

public class BufferAndHttpTests
{
    [Fact]
    public async Task ReadinessIsReportedOnTimeEvenWithoutUpdates()
    {
        var book = new OrderBook();
        var reports = Channel.CreateUnbounded<bool>();
        using var stopping = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var heartbeat = BinanceWorker.ReportReadinessAsync(
            () => book.IsReady, ready => reports.Writer.TryWrite(ready),
            TimeSpan.FromMilliseconds(20), stopping.Token);

        Assert.False(await reports.Reader.ReadAsync(stopping.Token));
        book.MarkReady();
        Assert.True(await reports.Reader.ReadAsync(stopping.Token));

        stopping.Cancel();
        await heartbeat;
    }

    [Fact]
    public async Task BufferPreservesOrderAndRefusesOverflow()
    {
        var buffer = new BufferedUpdates();
        for (var i = 0; i < 2_000; i++)
            buffer.Add([(byte)(i % 256)]);

        Assert.Throws<InvalidDataException>(() => buffer.Add([1]));
        Assert.Equal(2_000, buffer.Count);
        for (var i = 0; i < 2_000; i++)
            Assert.Equal((byte)(i % 256), (await buffer.ReadAsync(CancellationToken.None)).Payload[0]);
        Assert.Equal(0, buffer.Count);
    }

    [Fact]
    public async Task ANewAttemptCannotReadAnOldAttemptsQueue()
    {
        var old = new BufferedUpdates();
        old.Add([1]);
        old.Complete();
        var fresh = new BufferedUpdates();
        fresh.Add([2]);

        Assert.Equal(2, (await fresh.ReadAsync(CancellationToken.None)).Payload[0]);
        Assert.Equal(1, (await old.ReadAsync(CancellationToken.None)).Payload[0]);
        await Assert.ThrowsAsync<IOException>(() => old.ReadAsync(CancellationToken.None));
    }

    [Fact]
    public async Task ReceiverFailurePreventsReplayingQueuedUpdates()
    {
        var buffer = new BufferedUpdates();
        buffer.Add([1]);
        buffer.Complete(new IOException("connection failed"));

        var exception = await Assert.ThrowsAsync<IOException>(() => buffer.ReadAsync(CancellationToken.None));
        Assert.Contains("connection failed", exception.InnerException?.Message);
    }

    [Theory]
    [InlineData(HttpStatusCode.TooManyRequests)]
    [InlineData((HttpStatusCode)418)]
    public async Task RateLimitsUseRetryAfterSeconds(HttpStatusCode status)
    {
        using var http = new HttpClient(new Handler(() =>
        {
            var response = new HttpResponseMessage(status);
            response.Headers.TryAddWithoutValidation("Retry-After", "12");
            return response;
        }));
        var worker = new BinanceWorker(http, "BNBBTC");

        var exception = await Assert.ThrowsAsync<BinanceWorker.RateLimitedException>(
            () => worker.FetchSnapshotAsync(CancellationToken.None));
        Assert.Equal(TimeSpan.FromSeconds(12), exception.RetryAfter);
    }

    [Fact]
    public async Task MissingRateLimitHeaderStopsAutomaticRetry()
    {
        using var http = new HttpClient(new Handler(() => new HttpResponseMessage(HttpStatusCode.TooManyRequests)));
        var worker = new BinanceWorker(http, "BNBBTC");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => worker.FetchSnapshotAsync(CancellationToken.None));
        Assert.Contains("automatic retries stopped", exception.Message);
    }

    [Fact]
    public async Task SnapshotCanBeFetchedWhileUpdatesAreBuffered()
    {
        using var http = new HttpClient(new Handler(() => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(
                """{"lastUpdateId":157,"bids":[["4","431"]],"asks":[["4.000002","12"]]}""",
                Encoding.UTF8, "application/json")
        }));
        var buffer = new BufferedUpdates();
        buffer.Add(Encoding.UTF8.GetBytes(
            """{"e":"depthUpdate","s":"BNBBTC","U":158,"u":160,"b":[["4","0"]],"a":[]}"""));
        var worker = new BinanceWorker(http, "BNBBTC");

        var snapshot = await worker.FetchSnapshotAsync(CancellationToken.None);
        var received = await buffer.ReadAsync(CancellationToken.None);
        var update = BinanceMessages.ParseEvent(received.Payload, "BNBBTC");
        Assert.True(UpdateSequence.ShouldApply(snapshot.LastUpdateId, update));
    }

    private sealed class Handler(Func<HttpResponseMessage> response) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Assert.Contains("symbol=BNBBTC", request.RequestUri?.Query);
            return Task.FromResult(response());
        }
    }
}
