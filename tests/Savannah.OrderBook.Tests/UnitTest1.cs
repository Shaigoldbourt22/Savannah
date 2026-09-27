using System.Text;
using Savannah.OrderBook;

namespace Savannah.OrderBook.Tests;

public class OrderBookTests
{
    [Fact]
    public void AssignmentExampleAppliesAllChanges()
    {
        var book = new OrderBook();
        book.Load(new DepthSnapshot(157, [new(4m, 431m)], [new(4.000002m, 12m)]));
        var update = new DepthEvent("BNBBTC", 158, 160,
            [new(0.0024m, 10m), new(4m, 0m)], [new(0.0026m, 100m)]);

        Assert.True(UpdateSequence.ShouldApply(book.LastUpdateId, update));
        book.Apply(update);

        Assert.Equal(160, book.LastUpdateId);
        Assert.False(book.TryGetBid(4m, out _));
        Assert.True(book.TryGetBid(0.0024m, out var bid));
        Assert.Equal(10m, bid);
        Assert.True(book.TryGetAsk(4.000002m, out var oldAsk));
        Assert.Equal(12m, oldAsk);
        Assert.True(book.TryGetAsk(0.0026m, out var newAsk));
        Assert.Equal(100m, newAsk);
        Assert.Contains("0.0024 : 10", book.Format("BNBBTC"));
    }

    [Fact]
    public void ExistingPriceIsReplacedAndMissingZeroIsIgnored()
    {
        var book = new OrderBook();
        book.Load(new DepthSnapshot(50, [new(2m, 5m)], []));
        book.Apply(new DepthEvent("BNBBTC", 51, 51, [new(2m, 7m), new(3m, 0m)], []));
        Assert.True(book.TryGetBid(2m, out var quantity));
        Assert.Equal(7m, quantity);
        Assert.Equal(1, book.BidCount);
    }

    [Fact]
    public void InvalidUpdateDoesNotPartiallyChangeBook()
    {
        var book = new OrderBook();
        book.Load(new DepthSnapshot(50, [new(2m, 5m)], []));
        var invalid = new DepthEvent("BNBBTC", 51, 51, [new(2m, 7m), new(-1m, 9m)], []);

        Assert.Throws<InvalidDataException>(() => book.Apply(invalid));
        Assert.Equal(50, book.LastUpdateId);
        Assert.True(book.TryGetBid(2m, out var quantity));
        Assert.Equal(5m, quantity);
    }

    [Fact]
    public void BadSnapshotDoesNotReplaceBook()
    {
        var book = new OrderBook();
        book.Load(new DepthSnapshot(50, [new(2m, 5m)], []));

        Assert.Throws<InvalidDataException>(() =>
            book.Load(new DepthSnapshot(51, [new(2m, 6m), new(2m, 7m)], [])));
        Assert.Equal(50, book.LastUpdateId);
        Assert.True(book.TryGetBid(2m, out var quantity));
        Assert.Equal(5m, quantity);
    }

    [Theory]
    [InlineData(98, 100, false)]
    [InlineData(99, 101, true)]
    [InlineData(101, 101, true)]
    public void OldAndOverlappingUpdatesHaveExpectedSequence(long first, long last, bool expected)
    {
        var update = new DepthEvent("BNBBTC", first, last, [], []);
        Assert.Equal(expected, UpdateSequence.ShouldApply(100, update));
    }

    [Fact]
    public void MissingUpdateIsRejected()
    {
        var update = new DepthEvent("BNBBTC", 102, 103, [], []);
        Assert.Throws<InvalidDataException>(() => UpdateSequence.ShouldApply(100, update));
    }

    [Fact]
    public void PricesRetainSmallDecimalPlaces()
    {
        var snapshot = BinanceMessages.ParseSnapshot(Encoding.UTF8.GetBytes(
            """{"lastUpdateId":157,"bids":[["0.00240000","10.00000000"]],"asks":[]}"""));
        var book = new OrderBook();
        book.Load(snapshot);
        Assert.Contains("0.00240000 : 10.00000000", book.Format("BNBBTC"));
    }

    [Fact]
    public void OutputKeepsBidsDescendingAndAsksAscending()
    {
        var book = new OrderBook();
        book.Load(new DepthSnapshot(10,
            [new(2m, 3m), new(5m, 6m), new(1m, 2m)],
            [new(9m, 4m), new(7m, 8m), new(8m, 2m)]));
        var lines = book.Format("BNBBTC").Split(Environment.NewLine);

        Assert.Equal(["5 : 6", "2 : 3", "1 : 2"], lines.Skip(2).Take(3));
        Assert.Equal(["7 : 8", "8 : 2", "9 : 4"], lines.Skip(6).Take(3));
    }
}
