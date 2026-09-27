using Binance.LocalOrderBook;

namespace Binance.LocalOrderBook.Tests;

public class BookTelemetryTests
{
    [Fact]
    public void ReportsMedianAverageSnapshotCountAndHandledFraction()
    {
        var telemetry = new BookTelemetry();
        telemetry.RecordReceived();
        telemetry.RecordReceived();
        telemetry.RecordReceived();
        telemetry.RecordApplied(1);
        telemetry.RecordApplied(3);
        telemetry.RecordSkipped();
        telemetry.RecordSnapshotRequest();
        telemetry.RecordSnapshotRequest();
        telemetry.RecordSnapshotFailure();
        telemetry.RecordSnapshotLoaded();

        var report = telemetry.Drain();

        Assert.Equal(3, report.Received);
        Assert.Equal(2, report.Applied);
        Assert.Equal(1, report.Skipped);
        Assert.Equal(2, report.SnapshotRequests);
        Assert.Equal(1, report.SnapshotFailures);
        Assert.Equal(1, report.SnapshotsLoaded);
        Assert.Equal(2, report.ApplySamples);
        Assert.Equal(2, report.AverageApplyMs);
        Assert.Equal(2, report.MedianApplyMs);
        Assert.Equal(100, report.HandledPercent);
        Assert.Equal(200d / 3, report.AppliedPercent);
    }

    [Fact]
    public void LatenciesResetAfterReportingButCountersContinue()
    {
        var telemetry = new BookTelemetry();
        foreach (var duration in new[] { 1d, 5d, 9d })
        {
            telemetry.RecordReceived();
            telemetry.RecordApplied(duration);
        }
        Assert.Equal(5, telemetry.Drain().MedianApplyMs);

        var next = telemetry.Drain();
        Assert.Equal(0, next.ApplySamples);
        Assert.Null(next.AverageApplyMs);
        Assert.Null(next.MedianApplyMs);
        Assert.Equal(3, next.Received);
        Assert.Equal(3, next.Applied);
    }

    [Fact]
    public void LatencyWindowRemainsBounded()
    {
        var telemetry = new BookTelemetry();
        for (var i = 0; i < 10_001; i++)
        {
            telemetry.RecordReceived();
            telemetry.RecordApplied(i);
        }

        var report = telemetry.Drain();
        Assert.Equal(10_000, report.ApplySamples);
        Assert.Equal(10_001, report.Applied);
        Assert.Equal(5_000.5, report.MedianApplyMs);
        Assert.Equal(100, report.HandledPercent);
    }
}
