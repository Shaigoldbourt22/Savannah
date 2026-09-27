using System.Globalization;
using System.Text.Json;

namespace Savannah.OrderBook;

public static class BinanceMessages
{
    private const NumberStyles DecimalStyle = NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint;

    public static DepthSnapshot ParseSnapshot(ReadOnlyMemory<byte> payload)
    {
        using var document = JsonDocument.Parse(payload);
        var root = document.RootElement;
        return new DepthSnapshot(
            ReadId(root, "lastUpdateId"),
            ReadLevels(root, "bids"),
            ReadLevels(root, "asks"));
    }

    public static DepthEvent ParseEvent(ReadOnlyMemory<byte> payload, string expectedSymbol)
    {
        using var document = JsonDocument.Parse(payload);
        var root = document.RootElement;
        var kind = ReadString(root, "e");
        var symbol = ReadString(root, "s");
        if (kind != "depthUpdate" || !string.Equals(symbol, expectedSymbol, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Unexpected event type or trading pair.");

        var first = ReadId(root, "U");
        var last = ReadId(root, "u");
        if (first > last)
            throw new InvalidDataException("Event update ID range is invalid.");

        return new DepthEvent(symbol, first, last, ReadLevels(root, "b"), ReadLevels(root, "a"));
    }

    private static long ReadId(JsonElement root, string property)
    {
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty(property, out var value) ||
            value.ValueKind != JsonValueKind.Number ||
            !value.TryGetInt64(out var id) || id <= 0)
            throw new InvalidDataException($"Invalid {property} update ID.");
        return id;
    }

    private static string ReadString(JsonElement root, string property)
    {
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty(property, out var value) ||
            value.ValueKind != JsonValueKind.String ||
            string.IsNullOrWhiteSpace(value.GetString()))
            throw new InvalidDataException($"Invalid {property} field.");
        return value.GetString()!;
    }

    private static PriceLevel[] ReadLevels(JsonElement root, string property)
    {
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty(property, out var levels) ||
            levels.ValueKind != JsonValueKind.Array)
            throw new InvalidDataException($"Invalid {property} price levels.");

        var result = new List<PriceLevel>();
        foreach (var item in levels.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.Array || item.GetArrayLength() != 2)
                throw new InvalidDataException($"Invalid {property} price level.");
            var price = ReadDecimal(item[0], "price");
            var quantity = ReadDecimal(item[1], "quantity");
            if (price <= 0 || quantity < 0)
                throw new InvalidDataException($"Invalid {property} price or quantity.");
            result.Add(new PriceLevel(price, quantity));
        }
        return result.ToArray();
    }

    private static decimal ReadDecimal(JsonElement element, string field)
    {
        if (element.ValueKind != JsonValueKind.String)
            throw new InvalidDataException($"Invalid {field}: expected a string.");
        var text = element.GetString()!;
        var dot = text.IndexOf('.');
        if (dot >= 0 && text.Length - dot - 1 > 28)
            throw new InvalidDataException($"Invalid {field}: too many decimal places.");
        if (!decimal.TryParse(text, DecimalStyle, CultureInfo.InvariantCulture, out var value))
            throw new InvalidDataException($"Invalid {field}: not a representable decimal.");
        return value;
    }
}
