namespace Savannah.OrderBook;

internal sealed record BookTelemetrySnapshot(
    long Received,
    long Applied,
    long Skipped,
    long SnapshotRequests,
    long SnapshotFailures,
    long SnapshotsLoaded,
    long Resyncs,
    int ApplySamples,
    double? AverageApplyMs,
    double? MedianApplyMs)
{
    public double? HandledPercent => Received == 0 ? null : 100d * (Applied + Skipped) / Received;
    public double? AppliedPercent => Received == 0 ? null : 100d * Applied / Received;
}

internal sealed class BookTelemetry
{
    private const int MaximumSamples = 10_000;
    private readonly double[] _applyTimes = new double[MaximumSamples];
    private readonly object _sampleLock = new();
    private int _nextSample;
    private int _sampleCount;
    private long _received;
    private long _applied;
    private long _skipped;
    private long _snapshotRequests;
    private long _snapshotFailures;
    private long _snapshotsLoaded;
    private long _resyncs;

    public long Applied => Interlocked.Read(ref _applied);
    public long SnapshotFailures => Interlocked.Read(ref _snapshotFailures);
    public long Resyncs => Interlocked.Read(ref _resyncs);

    public void RecordReceived() => Interlocked.Increment(ref _received);
    public void RecordSkipped() => Interlocked.Increment(ref _skipped);
    public void RecordSnapshotRequest() => Interlocked.Increment(ref _snapshotRequests);
    public void RecordSnapshotFailure() => Interlocked.Increment(ref _snapshotFailures);
    public void RecordSnapshotLoaded() => Interlocked.Increment(ref _snapshotsLoaded);
    public void RecordResync() => Interlocked.Increment(ref _resyncs);

    public void RecordApplied(double elapsedMs)
    {
        if (!double.IsFinite(elapsedMs) || elapsedMs < 0)
            throw new ArgumentOutOfRangeException(nameof(elapsedMs));

        lock (_sampleLock)
        {
            _applyTimes[_nextSample] = elapsedMs;
            _nextSample = (_nextSample + 1) % MaximumSamples;
            _sampleCount = Math.Min(_sampleCount + 1, MaximumSamples);
        }
        Interlocked.Increment(ref _applied);
    }

    public BookTelemetrySnapshot Drain()
    {
        double[] samples;
        lock (_sampleLock)
        {
            samples = _applyTimes[.._sampleCount];
            _sampleCount = 0;
            _nextSample = 0;
        }

        Array.Sort(samples);
        var count = samples.Length;
        return new BookTelemetrySnapshot(
            Interlocked.Read(ref _received),
            Interlocked.Read(ref _applied),
            Interlocked.Read(ref _skipped),
            Interlocked.Read(ref _snapshotRequests),
            Interlocked.Read(ref _snapshotFailures),
            Interlocked.Read(ref _snapshotsLoaded),
            Interlocked.Read(ref _resyncs),
            count,
            count == 0 ? null : samples.Average(),
            count == 0 ? null : (samples[(count - 1) / 2] + samples[count / 2]) / 2);
    }
}
