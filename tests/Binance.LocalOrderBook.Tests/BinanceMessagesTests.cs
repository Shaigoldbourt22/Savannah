using System.Text;
using Binance.LocalOrderBook;

namespace Binance.LocalOrderBook.Tests;

public class BinanceMessagesTests
{
    [Fact]
    public void ParsesValidUpdateWithOverlappingIds()
    {
        var update = BinanceMessages.ParseEvent(Encoding.UTF8.GetBytes(
            """{"e":"depthUpdate","s":"BNBBTC","U":98,"u":101,"b":[["0.0024","10"]],"a":[]}"""),
            "BNBBTC");

        Assert.Equal(98, update.FirstUpdateId);
        Assert.Equal(101, update.FinalUpdateId);
        Assert.Equal(new PriceLevel(0.0024m, 10m), update.Bids[0]);
    }

    [Theory]
    [InlineData("""{"e":"depthUpdate","s":"ETHBTC","U":1,"u":1,"b":[],"a":[]}""")]
    [InlineData("""{"e":"depthUpdate","s":"BNBBTC","U":3,"u":2,"b":[],"a":[]}""")]
    [InlineData("""{"e":"depthUpdate","s":"BNBBTC","U":1,"u":1,"b":[["1","-1"]],"a":[]}""")]
    [InlineData("""{"e":"depthUpdate","s":"BNBBTC","U":1,"u":1,"b":[["0","2"]],"a":[]}""")]
    [InlineData("""{"e":"depthUpdate","s":"BNBBTC","U":1,"u":1,"b":[["1.12345678901234567890123456789","2"]],"a":[]}""")]
    [InlineData("""{"e":"depthUpdate","s":"BNBBTC","U":1,"u":1,"b":[[1,"2"]],"a":[]}""")]
    [InlineData("""{"e":"depthUpdate","s":"BNBBTC","U":1,"u":1,"b":[] }""")]
    public void InvalidEventIsRejectedBeforeApplying(string json)
    {
        Assert.Throws<InvalidDataException>(() =>
            BinanceMessages.ParseEvent(Encoding.UTF8.GetBytes(json), "BNBBTC"));
    }

    [Fact]
    public void SnapshotRejectsInvalidPriceLevel()
    {
        Assert.Throws<InvalidDataException>(() => BinanceMessages.ParseSnapshot(Encoding.UTF8.GetBytes(
            """{"lastUpdateId":100,"bids":[["1","1"],["-1","2"]],"asks":[]}""")));
    }
}
