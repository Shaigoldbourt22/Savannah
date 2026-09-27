using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.WebSockets;

namespace Binance.LocalOrderBook;

public sealed class BinanceWorker(HttpClient httpClient, string symbol)
{
    private readonly OrderBook _book = new();
    private readonly BookTelemetry _telemetry = new();
    private long _lastReportedSnapshotRequests;

    public OrderBook Book => _book;

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        using var heartbeatStop = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var heartbeat = ReportReadinessAsync(
            () => _book.IsReady,
            ready =>
            {
                Console.WriteLine(
                    $"readiness symbol={symbol} book_ready={(ready ? 1 : 0)} lastUpdateId={_book.LastUpdateId}");
                LogTelemetry();
            },
            TimeSpan.FromSeconds(30), heartbeatStop.Token);
        try
        {
            await RunUpdatesAsync(stoppingToken);
        }
        finally
        {
            heartbeatStop.Cancel();
            await heartbeat;
        }
    }

    internal static async Task ReportReadinessAsync(
        Func<bool> isReady, Action<bool> report, TimeSpan interval, CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
                report(isReady());
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task RunUpdatesAsync(CancellationToken stoppingToken)
    {
        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            using var attempt = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            using var socket = new ClientWebSocket();
            socket.Options.KeepAliveInterval = TimeSpan.FromSeconds(20);
            var updates = new BufferedUpdates();
            Task? receiver = null;
            var delay = TimeSpan.Zero;
            _book.Invalidate();

            try
            {
                var streamUrl = new Uri($"wss://data-stream.binance.vision/ws/{symbol.ToLowerInvariant()}@depth@100ms");
                await socket.ConnectAsync(streamUrl, attempt.Token);
                receiver = ReceiveAsync(socket, updates, attempt.Token);
                Console.WriteLine($"Connected to Binance for {symbol}; book_ready=0");

                var first = await updates.ReadAsync(attempt.Token);
                var firstEvent = BinanceMessages.ParseEvent(first.Payload, symbol);
                DepthSnapshot snapshot;
                do
                {
                    snapshot = await FetchSnapshotAsync(attempt.Token);
                } while (snapshot.LastUpdateId < firstEvent.FirstUpdateId);

                _book.Load(snapshot);
                _telemetry.RecordSnapshotLoaded();
                Console.WriteLine($"Snapshot loaded for {symbol}: lastUpdateId={snapshot.LastUpdateId}");

                var hasBridged = false;
                Apply(first, updates, ref hasBridged);
                if (hasBridged)
                    failures = 0;
                while (!attempt.IsCancellationRequested)
                {
                    var next = await updates.ReadAsync(attempt.Token);
                    Apply(next, updates, ref hasBridged);
                    if (hasBridged)
                        failures = 0;
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (RateLimitedException exception)
            {
                delay = exception.RetryAfter;
                Console.Error.WriteLine($"Binance rate limit for {symbol}: waiting {delay.TotalSeconds} seconds.");
            }
            catch (Exception exception) when (exception is HttpRequestException or WebSocketException or
                IOException or InvalidDataException or TimeoutException or System.Text.Json.JsonException)
            {
                failures++;
                _telemetry.RecordResync();
                delay = TimeSpan.FromSeconds(Math.Min(30, Math.Pow(2, Math.Min(failures - 1, 5))) *
                    (0.75 + Random.Shared.NextDouble() * 0.5));
                Console.Error.WriteLine($"Resynchronizing {symbol}: {exception.Message}");
            }
            finally
            {
                _book.Invalidate();
                attempt.Cancel();
                socket.Abort();
                if (receiver is not null)
                    await receiver;
                Console.WriteLine($"book_ready=0 symbol={symbol} resync_total={_telemetry.Resyncs}");
            }

            if (delay > TimeSpan.Zero)
                await Task.Delay(delay, stoppingToken);
        }
    }

    private void Apply(ReceivedUpdate received, BufferedUpdates updates, ref bool hasBridged)
    {
        if (updates.Failure is { } failure)
            throw new IOException("Binance update stream failed.", failure);
        var update = BinanceMessages.ParseEvent(received.Payload, symbol);
        if (!UpdateSequence.ShouldApply(_book.LastUpdateId, update))
        {
            _telemetry.RecordSkipped();
            return;
        }

        if (!hasBridged)
            Console.WriteLine("BEFORE UPDATE" + Environment.NewLine + _book.Format(symbol));

        var applyStart = Stopwatch.GetTimestamp();
        _book.Apply(update);
        _telemetry.RecordApplied(Stopwatch.GetElapsedTime(applyStart).TotalMilliseconds);
        if (!hasBridged)
        {
            hasBridged = true;
            _book.MarkReady();
            Console.WriteLine("AFTER UPDATE" + Environment.NewLine + _book.Format(symbol));
            Console.WriteLine($"book_ready=1 symbol={symbol}");
        }

        if (_telemetry.Applied % 100 == 0)
        {
            var elapsed = Stopwatch.GetElapsedTime(received.ReceivedAt).TotalMilliseconds;
            Console.WriteLine($"metrics symbol={symbol} book_ready=1 buffered_updates={updates.Count} " +
                $"update_processing_delay_ms={elapsed.ToString("F3", CultureInfo.InvariantCulture)} " +
                $"resync_total={_telemetry.Resyncs} snapshot_failures_total={_telemetry.SnapshotFailures}");
        }
    }

    private void LogTelemetry()
    {
        var report = _telemetry.Drain();
        var snapshotRequests = report.SnapshotRequests - _lastReportedSnapshotRequests;
        _lastReportedSnapshotRequests = report.SnapshotRequests;
        Console.WriteLine(
            $"telemetry symbol={symbol} received_total={report.Received} applied_total={report.Applied} " +
            $"skipped_total={report.Skipped} handled_pct_received={Format(report.HandledPercent, "F2")} " +
            $"applied_pct_received={Format(report.AppliedPercent, "F2")} " +
            $"snapshot_requests_total={report.SnapshotRequests} snapshot_requests_since_report={snapshotRequests} " +
            $"snapshots_loaded_total={report.SnapshotsLoaded} snapshot_failures_total={report.SnapshotFailures} " +
            $"apply_samples={report.ApplySamples} apply_avg_ms={Format(report.AverageApplyMs, "F3")} " +
            $"apply_median_ms={Format(report.MedianApplyMs, "F3")} resync_total={report.Resyncs}");
    }

    private static string Format(double? value, string format) =>
        value?.ToString(format, CultureInfo.InvariantCulture) ?? "n/a";

    internal async Task<DepthSnapshot> FetchSnapshotAsync(CancellationToken stoppingToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(5));
        try
        {
            var url = $"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=5000";
            _telemetry.RecordSnapshotRequest();
            using var response = await httpClient.GetAsync(url, timeout.Token);
            if (response.StatusCode == HttpStatusCode.TooManyRequests || (int)response.StatusCode == 418)
            {
                _telemetry.RecordSnapshotFailure();
                if (!response.Headers.TryGetValues("Retry-After", out var headers) ||
                    headers.Count() != 1 ||
                    !int.TryParse(headers.Single(), NumberStyles.None, CultureInfo.InvariantCulture,
                        out var seconds) || seconds <= 0)
                    throw new InvalidOperationException("Binance rate limit has no valid Retry-After; automatic retries stopped.");
                throw new RateLimitedException(TimeSpan.FromSeconds(seconds));
            }

            if ((int)response.StatusCode == 451)
            {
                _telemetry.RecordSnapshotFailure();
                throw new InvalidOperationException("Binance market data is unavailable from this Azure region (HTTP 451).");
            }

            response.EnsureSuccessStatusCode();
            var payload = await response.Content.ReadAsByteArrayAsync(timeout.Token);
            return BinanceMessages.ParseSnapshot(payload);
        }
        catch (OperationCanceledException exception) when (!stoppingToken.IsCancellationRequested)
        {
            _telemetry.RecordSnapshotFailure();
            throw new TimeoutException("Binance snapshot request timed out.", exception);
        }
        catch (HttpRequestException)
        {
            _telemetry.RecordSnapshotFailure();
            throw;
        }
        catch (System.Text.Json.JsonException)
        {
            _telemetry.RecordSnapshotFailure();
            throw;
        }
        catch (InvalidDataException)
        {
            _telemetry.RecordSnapshotFailure();
            throw;
        }
    }

    private async Task ReceiveAsync(ClientWebSocket socket, BufferedUpdates updates, CancellationToken token)
    {
        var buffer = new byte[64 * 1024];
        using var message = new MemoryStream();
        try
        {
            while (!token.IsCancellationRequested)
            {
                var part = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), token);
                if (part.MessageType == WebSocketMessageType.Close)
                    throw new IOException("Binance closed the WebSocket.");
                if (part.MessageType != WebSocketMessageType.Text)
                    throw new InvalidDataException("Unexpected Binance WebSocket message type.");
                if (message.Length + part.Count > 16 * 1024 * 1024)
                    throw new InvalidDataException("Binance WebSocket message exceeds the buffer limit.");

                message.Write(buffer, 0, part.Count);
                if (!part.EndOfMessage)
                    continue;

                _telemetry.RecordReceived();
                updates.Add(message.ToArray());
                message.SetLength(0);
            }
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            updates.Complete();
            return;
        }
        catch (Exception exception) when (exception is WebSocketException or IOException or InvalidDataException)
        {
            Console.Error.WriteLine($"Binance receiver stopped: {exception.Message}");
            _book.Invalidate();
            updates.Complete(exception);
            return;
        }
        catch (Exception exception)
        {
            _book.Invalidate();
            updates.Complete(exception);
            throw;
        }
        updates.Complete();
    }

    internal sealed class RateLimitedException(TimeSpan retryAfter) : Exception("Binance REST rate limit")
    {
        public TimeSpan RetryAfter { get; } = retryAfter;
    }
}
