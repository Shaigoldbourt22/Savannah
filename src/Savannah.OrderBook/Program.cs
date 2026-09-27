using System.Text.RegularExpressions;
using Savannah.OrderBook;

if (args.Length > 1)
{
    Console.Error.WriteLine("Usage: Savannah.OrderBook [SYMBOL]");
    return 2;
}

var symbol = (args.Length == 1 ? args[0] : Environment.GetEnvironmentVariable("SYMBOL") ?? "BNBBTC")
    .ToUpperInvariant();
if (!Regex.IsMatch(symbol, "^[A-Z0-9]{5,20}$", RegexOptions.CultureInvariant))
{
    Console.Error.WriteLine("SYMBOL must contain 5-20 letters or digits, such as BNBBTC.");
    return 2;
}

using var httpClient = new HttpClient();
using var stopping = new CancellationTokenSource();
Console.CancelKeyPress += (_, eventArgs) =>
{
    eventArgs.Cancel = true;
    stopping.Cancel();
};

try
{
    await new BinanceWorker(httpClient, symbol).RunAsync(stopping.Token);
    return 0;
}
catch (OperationCanceledException) when (stopping.IsCancellationRequested)
{
    return 0;
}
catch (InvalidOperationException exception)
{
    Console.Error.WriteLine($"Worker stopped: {exception.Message}");
    return 1;
}
