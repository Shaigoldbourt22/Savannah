using System.Diagnostics;
using System.Threading.Channels;

namespace Savannah.OrderBook;

internal readonly record struct ReceivedUpdate(byte[] Payload, long ReceivedAt);

internal sealed class BufferedUpdates
{
    private const int MaximumEvents = 2_000;
    private const int MaximumBytes = 16 * 1024 * 1024;
    private readonly Channel<ReceivedUpdate> _channel = Channel.CreateUnbounded<ReceivedUpdate>(
        new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });
    private int _count;
    private long _bytes;
    private Exception? _failure;

    public int Count => Volatile.Read(ref _count);
    public Exception? Failure => Volatile.Read(ref _failure);

    public void Add(byte[] payload)
    {
        ArgumentNullException.ThrowIfNull(payload);
        if (Interlocked.Increment(ref _count) > MaximumEvents)
        {
            Interlocked.Decrement(ref _count);
            throw new InvalidDataException("Update buffer limit reached.");
        }

        if (Interlocked.Add(ref _bytes, payload.Length) > MaximumBytes)
        {
            Interlocked.Add(ref _bytes, -payload.Length);
            Interlocked.Decrement(ref _count);
            throw new InvalidDataException("Update buffer limit reached.");
        }

        if (!_channel.Writer.TryWrite(new ReceivedUpdate(payload, Stopwatch.GetTimestamp())))
        {
            Interlocked.Add(ref _bytes, -payload.Length);
            Interlocked.Decrement(ref _count);
            throw new IOException("Update stream has stopped.");
        }
    }

    public void Complete(Exception? error = null)
    {
        if (error is not null)
            Volatile.Write(ref _failure, error);
        _channel.Writer.TryComplete(error);
    }

    public async Task<ReceivedUpdate> ReadAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (Failure is { } failure)
                throw new IOException("Binance update stream failed.", failure);
            var update = await _channel.Reader.ReadAsync(cancellationToken);
            if (Failure is { } readFailure)
                throw new IOException("Binance update stream failed.", readFailure);
            Interlocked.Decrement(ref _count);
            Interlocked.Add(ref _bytes, -update.Payload.Length);
            return update;
        }
        catch (ChannelClosedException exception)
        {
            throw new IOException("Binance update stream ended.", exception.InnerException ?? exception);
        }
    }
}
