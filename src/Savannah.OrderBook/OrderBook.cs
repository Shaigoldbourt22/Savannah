using System.Globalization;

namespace Savannah.OrderBook;

public readonly record struct PriceLevel(decimal Price, decimal Quantity);

public sealed record DepthSnapshot(long LastUpdateId, IReadOnlyList<PriceLevel> Bids, IReadOnlyList<PriceLevel> Asks);

public sealed record DepthEvent(
    string Symbol,
    long FirstUpdateId,
    long FinalUpdateId,
    IReadOnlyList<PriceLevel> Bids,
    IReadOnlyList<PriceLevel> Asks);

public static class UpdateSequence
{
    public static bool ShouldApply(long bookId, DepthEvent update)
    {
        if (bookId <= 0 || bookId == long.MaxValue || update.FirstUpdateId <= 0 ||
            update.FirstUpdateId > update.FinalUpdateId)
            throw new InvalidDataException("Invalid update ID range.");
        if (update.FinalUpdateId <= bookId)
            return false;
        if (update.FirstUpdateId > bookId + 1)
            throw new InvalidDataException($"Update gap: expected {bookId + 1}, got {update.FirstUpdateId}.");
        return true;
    }
}

public sealed class OrderBook
{
    private SortedDictionary<decimal, decimal> _bids = new(Comparer<decimal>.Create((a, b) => b.CompareTo(a)));
    private SortedDictionary<decimal, decimal> _asks = new();
    private bool _isReady;
    private long _lastUpdateId;

    public long LastUpdateId => Interlocked.Read(ref _lastUpdateId);
    public bool IsReady => Volatile.Read(ref _isReady);
    public int BidCount => _bids.Count;
    public int AskCount => _asks.Count;

    public bool TryGetBid(decimal price, out decimal quantity) => _bids.TryGetValue(price, out quantity);
    public bool TryGetAsk(decimal price, out decimal quantity) => _asks.TryGetValue(price, out quantity);

    public void Invalidate() => Volatile.Write(ref _isReady, false);

    public void MarkReady() => Volatile.Write(ref _isReady, true);

    public void Load(DepthSnapshot snapshot)
    {
        if (snapshot.LastUpdateId <= 0 || snapshot.LastUpdateId == long.MaxValue)
            throw new InvalidDataException("Snapshot update ID must be positive.");

        var bids = new SortedDictionary<decimal, decimal>(Comparer<decimal>.Create((a, b) => b.CompareTo(a)));
        var asks = new SortedDictionary<decimal, decimal>();
        LoadSide(snapshot.Bids, bids);
        LoadSide(snapshot.Asks, asks);

        _bids = bids;
        _asks = asks;
        Interlocked.Exchange(ref _lastUpdateId, snapshot.LastUpdateId);
        Invalidate();
    }

    public void Apply(DepthEvent update)
    {
        if (update.FinalUpdateId <= LastUpdateId)
            throw new InvalidDataException("The update ID must advance.");

        ValidateLevels(update.Bids);
        ValidateLevels(update.Asks);

        foreach (var level in update.Bids)
            SetLevel(_bids, level);
        foreach (var level in update.Asks)
            SetLevel(_asks, level);

        Interlocked.Exchange(ref _lastUpdateId, update.FinalUpdateId);
    }

    public string Format(string symbol, int depth = 5)
    {
        if (depth <= 0)
            throw new ArgumentOutOfRangeException(nameof(depth));

        var bidLines = _bids.Take(depth).Select(FormatLevel);
        var askLines = _asks.Take(depth).Select(FormatLevel);
        return $"ORDER BOOK {symbol} | lastUpdateId={LastUpdateId} | known levels (bids={BidCount}, asks={AskCount})"
            + Environment.NewLine + "BIDS:" + Environment.NewLine
            + string.Join(Environment.NewLine, bidLines) + Environment.NewLine + "ASKS:" + Environment.NewLine
            + string.Join(Environment.NewLine, askLines);
    }

    private static string FormatLevel(KeyValuePair<decimal, decimal> level) =>
        $"{level.Key.ToString(CultureInfo.InvariantCulture)} : {level.Value.ToString(CultureInfo.InvariantCulture)}";

    private static void LoadSide(IReadOnlyList<PriceLevel> levels, SortedDictionary<decimal, decimal> side)
    {
        ValidateLevels(levels);
        foreach (var level in levels)
        {
            if (level.Quantity != 0 && !side.TryAdd(level.Price, level.Quantity))
                throw new InvalidDataException("Duplicate price in snapshot.");
        }
    }

    private static void ValidateLevels(IReadOnlyList<PriceLevel> levels)
    {
        ArgumentNullException.ThrowIfNull(levels);
        foreach (var level in levels)
        {
            if (level.Price <= 0 || level.Quantity < 0)
                throw new InvalidDataException("Prices must be positive and quantities must be nonnegative.");
        }
    }

    private static void SetLevel(SortedDictionary<decimal, decimal> side, PriceLevel level)
    {
        if (level.Quantity == 0)
            side.Remove(level.Price);
        else
            side[level.Price] = level.Quantity;
    }
}
