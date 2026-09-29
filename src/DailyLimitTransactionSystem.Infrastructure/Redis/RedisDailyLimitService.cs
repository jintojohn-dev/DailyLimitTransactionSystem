using System.Globalization;
using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DailyLimitTransactionSystem.Infrastructure.Redis;

public class RedisDailyLimitService : IDailyLimitService
{
    private readonly IConnectionMultiplexer? _redis;
    private readonly ILogger<RedisDailyLimitService> _logger;

    // Fallback thread-safe in-memory cache when Redis is not reachable / mock mode
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, (decimal Spend, DateTime Expiry)> _inMemoryStore = new();
    private static readonly object _inMemoryLock = new();

    public RedisDailyLimitService(ILogger<RedisDailyLimitService> logger, IConnectionMultiplexer? redis = null)
    {
        _logger = logger;
        _redis = redis;
    }

    private static string GetDailyLimitKey(string userId, DateOnly date) => $"limit:user:{userId}:{date:yyyy-MM-dd}";

    public async Task<DailyLimitCheckResult> TryDeductDailyLimitAsync(
        string userId,
        DateOnly date,
        decimal amount,
        decimal dailyLimit,
        TimeSpan? ttl = null,
        CancellationToken ct = default)
    {
        var key = GetDailyLimitKey(userId, date);
        var effectiveTtl = ttl ?? TimeSpan.FromHours(48); // 48h covers end-of-day + buffer

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();
            var keys = new RedisKey[] { key };
            var values = new RedisValue[]
            {
                amount.ToString(CultureInfo.InvariantCulture),
                dailyLimit.ToString(CultureInfo.InvariantCulture),
                ((long)effectiveTtl.TotalSeconds).ToString()
            };

            var evalResult = await db.ScriptEvaluateAsync(
                LuaScripts.CheckAndDeductLimit,
                keys,
                values
            );

            var arrayResult = (RedisResult[])evalResult!;
            int isAllowed = (int)arrayResult[0];
            decimal totalSpend = decimal.Parse((string)arrayResult[1]!, CultureInfo.InvariantCulture);
            decimal remaining = decimal.Parse((string)arrayResult[2]!, CultureInfo.InvariantCulture);

            _logger.LogInformation(
                "[Redis Lua Engine] User: {UserId}, Date: {Date}, Attempted: ${Amount:N2}, Allowed: {Allowed}, New Total Spend: ${TotalSpend:N2}, Remaining Limit: ${Remaining:N2}",
                userId, date, amount, isAllowed == 1, totalSpend, remaining
            );

            return new DailyLimitCheckResult
            {
                IsAllowed = isAllowed == 1,
                AmountRequested = amount,
                CurrentTotalSpent = totalSpend,
                DailyLimit = dailyLimit,
                RemainingLimit = remaining,
                Reason = isAllowed == 1 ? null : $"Daily limit of ${dailyLimit:N2} would be exceeded. Current spend: ${totalSpend:N2} + Requested: ${amount:N2} > Limit"
            };
        }

        // Exact Atomic semantics for In-Memory / Test / Offline mode:
        lock (_inMemoryLock)
        {
            var now = DateTime.UtcNow;
            if (_inMemoryStore.TryGetValue(key, out var record) && record.Expiry < now)
            {
                _inMemoryStore.TryRemove(key, out _);
                record = (0m, now.Add(effectiveTtl));
            }
            else if (!record.Equals(default))
            {
                // existing active
            }
            else
            {
                record = (0m, now.Add(effectiveTtl));
            }

            decimal currentSpend = record.Spend;
            if (currentSpend + amount <= dailyLimit + 0.000001m)
            {
                decimal newSpend = currentSpend + amount;
                _inMemoryStore[key] = (newSpend, record.Expiry);
                decimal remaining = Math.Max(0, dailyLimit - newSpend);

                _logger.LogInformation(
                    "[Atomic Engine (In-Memory)] User: {UserId}, Attempted: ${Amount:N2}, Allowed: TRUE, Total Spend: ${NewSpend:N2}, Remaining: ${Remaining:N2}",
                    userId, amount, newSpend, remaining
                );

                return new DailyLimitCheckResult
                {
                    IsAllowed = true,
                    AmountRequested = amount,
                    CurrentTotalSpent = newSpend,
                    DailyLimit = dailyLimit,
                    RemainingLimit = remaining
                };
            }
            else
            {
                decimal remaining = Math.Max(0, dailyLimit - currentSpend);
                _logger.LogWarning(
                    "[Atomic Engine (In-Memory)] User: {UserId}, Attempted: ${Amount:N2}, Allowed: FALSE (Exceeded). Current Spend: ${CurrentSpend:N2}, Remaining: ${Remaining:N2}",
                    userId, amount, currentSpend, remaining
                );

                return new DailyLimitCheckResult
                {
                    IsAllowed = false,
                    AmountRequested = amount,
                    CurrentTotalSpent = currentSpend,
                    DailyLimit = dailyLimit,
                    RemainingLimit = remaining,
                    Reason = $"Daily limit of ${dailyLimit:N2} exceeded. Current spend: ${currentSpend:N2}, Attempted: ${amount:N2}"
                };
            }
        }
    }

    public async Task<decimal> GetCurrentDailySpendAsync(string userId, DateOnly date, CancellationToken ct = default)
    {
        var key = GetDailyLimitKey(userId, date);

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();
            var val = await db.StringGetAsync(key);
            if (val.IsNullOrEmpty) return 0m;
            return decimal.Parse((string)val!, CultureInfo.InvariantCulture);
        }

        lock (_inMemoryLock)
        {
            if (_inMemoryStore.TryGetValue(key, out var record))
            {
                if (record.Expiry >= DateTime.UtcNow) return record.Spend;
                _inMemoryStore.TryRemove(key, out _);
            }
            return 0m;
        }
    }

    public async Task RollbackDailyLimitAsync(string userId, DateOnly date, decimal amount, CancellationToken ct = default)
    {
        var key = GetDailyLimitKey(userId, date);

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();
            await db.ScriptEvaluateAsync(
                LuaScripts.RollbackLimit,
                new RedisKey[] { key },
                new RedisValue[] { amount.ToString(CultureInfo.InvariantCulture) }
            );
            return;
        }

        lock (_inMemoryLock)
        {
            if (_inMemoryStore.TryGetValue(key, out var record))
            {
                var newSpend = Math.Max(0, record.Spend - amount);
                _inMemoryStore[key] = (newSpend, record.Expiry);
            }
        }
    }

    public async Task ResetDailyLimitAsync(string userId, DateOnly date, CancellationToken ct = default)
    {
        var key = GetDailyLimitKey(userId, date);

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();
            await db.KeyDeleteAsync(key);
            return;
        }

        lock (_inMemoryLock)
        {
            _inMemoryStore.TryRemove(key, out _);
        }
    }
}
