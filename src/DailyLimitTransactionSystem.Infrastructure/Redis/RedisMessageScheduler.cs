using System.Globalization;
using System.Text.Json;
using DailyLimitTransactionSystem.Core.Models;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DailyLimitTransactionSystem.Infrastructure.Redis;

/// <summary>
/// Lightweight Redis-backed Scheduler for Azure Service Bus Basic Tier.
/// Uses Redis Sorted Sets (ZADD / ZRANGEBYSCORE) to store future scheduled transactions
/// and dispatch them when their execution time arrives.
/// 
/// CRITICAL FIX (Bug #2 — Creation-Order FIFO):
/// Once due transactions are fetched, they are sorted by CreatedAt (creation timestamp)
/// to ensure the earliest-created scheduled e-Transfer is processed first.
/// This prevents the scenario where a later-created transfer consumes the daily limit
/// and causes an earlier-created transfer to be rejected.
/// </summary>
public class RedisMessageScheduler
{
    private readonly IConnectionMultiplexer? _redis;
    private readonly ILogger<RedisMessageScheduler> _logger;
    private const string ScheduledSetKey = "scheduler:transactions:due";

    // In-memory fallback sorted set for offline/test mode
    private static readonly SortedDictionary<long, List<Transaction>> _inMemorySchedule = new();
    private static readonly object _inMemoryLock = new();

    public RedisMessageScheduler(ILogger<RedisMessageScheduler> logger, IConnectionMultiplexer? redis = null)
    {
        _logger = logger;
        _redis = redis;
    }

    public async Task ScheduleAsync(Transaction transaction, CancellationToken ct = default)
    {
        long score = new DateTimeOffset(transaction.ScheduledExecutionTime).ToUnixTimeSeconds();
        var json = JsonSerializer.Serialize(transaction);

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();
            await db.SortedSetAddAsync(ScheduledSetKey, json, score);
            _logger.LogInformation(
                "[Redis Basic Scheduler] Scheduled Tx {TxId} (${Amount:N2}) for {Time:yyyy-MM-dd HH:mm:ss} (Created: {Created:yyyy-MM-dd HH:mm:ss.fff}) in Redis Sorted Set",
                transaction.Id[..8], transaction.Amount, transaction.ScheduledExecutionTime, transaction.CreatedAt
            );
            return;
        }

        lock (_inMemoryLock)
        {
            if (!_inMemorySchedule.TryGetValue(score, out var list))
            {
                list = new List<Transaction>();
                _inMemorySchedule[score] = list;
            }
            list.Add(transaction);
        }

        _logger.LogInformation(
            "[Redis Basic Scheduler (In-Memory)] Scheduled Tx {TxId} (${Amount:N2}) for {Time:yyyy-MM-dd HH:mm:ss} (Created: {Created:yyyy-MM-dd HH:mm:ss.fff})",
            transaction.Id[..8], transaction.Amount, transaction.ScheduledExecutionTime, transaction.CreatedAt
        );
    }

    /// <summary>
    /// Fetches all due transactions (score &lt;= asOfTime) and returns them sorted by CreatedAt.
    /// 
    /// The sorted set score (ScheduledExecutionTime) determines WHEN a transaction becomes due.
    /// Once due, transactions are dispatched in CREATION ORDER (FIFO by CreatedAt) to ensure
    /// the earliest-scheduled e-Transfer is processed first when multiple target the same date.
    /// 
    /// This directly fixes Bug #2: "Scheduled e-Transfers not processed in creation order".
    /// </summary>
    public async Task<IReadOnlyList<Transaction>> GetAndPollDueTransactionsAsync(DateTime asOfTime, CancellationToken ct = default)
    {
        long maxScore = new DateTimeOffset(asOfTime).ToUnixTimeSeconds();
        var result = new List<Transaction>();

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();
            // Fetch all transactions with scheduled time <= asOfTime
            var entries = await db.SortedSetRangeByScoreAsync(ScheduledSetKey, start: 0, stop: maxScore);
            
            foreach (var item in entries)
            {
                if (item.HasValue)
                {
                    var tx = JsonSerializer.Deserialize<Transaction>((string)item!);
                    if (tx != null)
                    {
                        result.Add(tx);
                        await db.SortedSetRemoveAsync(ScheduledSetKey, item);
                    }
                }
            }

            // CREATION-ORDER FIFO: Sort by CreatedAt so earliest-created transfer runs first
            result.Sort((a, b) => a.CreatedAt.CompareTo(b.CreatedAt));

            _logger.LogInformation(
                "[Redis Basic Scheduler] Dispatching {Count} due transactions in creation-order (FIFO by CreatedAt)",
                result.Count
            );

            return result;
        }

        lock (_inMemoryLock)
        {
            var dueKeys = _inMemorySchedule.Keys.Where(k => k <= maxScore).OrderBy(k => k).ToList();
            foreach (var key in dueKeys)
            {
                result.AddRange(_inMemorySchedule[key]);
                _inMemorySchedule.Remove(key);
            }
        }

        // CREATION-ORDER FIFO: Sort by CreatedAt so earliest-created transfer runs first
        result.Sort((a, b) => a.CreatedAt.CompareTo(b.CreatedAt));

        _logger.LogInformation(
            "[Redis Basic Scheduler (In-Memory)] Dispatching {Count} due transactions in creation-order (FIFO by CreatedAt)",
            result.Count
        );

        return result;
    }
}
