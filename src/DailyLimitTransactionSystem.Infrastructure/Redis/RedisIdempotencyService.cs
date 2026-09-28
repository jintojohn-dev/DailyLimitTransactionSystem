using System.Text.Json;
using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DailyLimitTransactionSystem.Infrastructure.Redis;

public class RedisIdempotencyService : IIdempotencyService
{
    private readonly IConnectionMultiplexer? _redis;
    private readonly ILogger<RedisIdempotencyService> _logger;
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, (string Status, TransactionResult? Result, DateTime Expiry)> _inMemoryStore = new();

    public RedisIdempotencyService(ILogger<RedisIdempotencyService> logger, IConnectionMultiplexer? redis = null)
    {
        _logger = logger;
        _redis = redis;
    }

    public async Task<bool> TryAcquireExecutionAsync(string transactionId, TimeSpan expiry, CancellationToken ct = default)
    {
        var key = $"idempotency:tx:{transactionId}:status";

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();
            return await db.StringSetAsync(key, "PROCESSING", expiry, When.NotExists);
        }

        var now = DateTime.UtcNow;
        if (_inMemoryStore.TryGetValue(key, out var existing))
        {
            if (existing.Expiry > now)
            {
                return false; // Already processing or completed
            }
        }

        return _inMemoryStore.TryAdd(key, ("PROCESSING", null, now.Add(expiry)));
    }

    public async Task MarkCompletedAsync(string transactionId, TransactionResult result, TimeSpan expiry, CancellationToken ct = default)
    {
        var statusKey = $"idempotency:tx:{transactionId}:status";
        var resultKey = $"idempotency:tx:{transactionId}:result";
        var json = JsonSerializer.Serialize(result);

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();
            var batch = db.CreateBatch();
            _ = batch.StringSetAsync(statusKey, "COMPLETED", expiry);
            _ = batch.StringSetAsync(resultKey, json, expiry);
            batch.Execute();
            return;
        }

        _inMemoryStore[statusKey] = ("COMPLETED", result, DateTime.UtcNow.Add(expiry));
        await Task.CompletedTask;
    }

    public async Task<TransactionResult?> GetCompletedResultAsync(string transactionId, CancellationToken ct = default)
    {
        var resultKey = $"idempotency:tx:{transactionId}:result";

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();
            var val = await db.StringGetAsync(resultKey);
            if (val.IsNullOrEmpty) return null;
            return JsonSerializer.Deserialize<TransactionResult>((string)val!);
        }

        var statusKey = $"idempotency:tx:{transactionId}:status";
        if (_inMemoryStore.TryGetValue(statusKey, out var record) && record.Result != null)
        {
            return record.Result;
        }

        return null;
    }
}
