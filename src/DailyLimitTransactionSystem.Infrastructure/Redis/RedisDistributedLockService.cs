using DailyLimitTransactionSystem.Core.Interfaces;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DailyLimitTransactionSystem.Infrastructure.Redis;

public class RedisDistributedLockService : IDistributedLockService
{
    private readonly IConnectionMultiplexer? _redis;
    private readonly ILogger<RedisDistributedLockService> _logger;
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, SemaphoreSlim> _inMemoryLocks = new();

    public RedisDistributedLockService(ILogger<RedisDistributedLockService> logger, IConnectionMultiplexer? redis = null)
    {
        _logger = logger;
        _redis = redis;
    }

    public async Task<IAsyncDisposable?> AcquireLockAsync(
        string resourceKey,
        TimeSpan expiry,
        TimeSpan waitTimeout,
        CancellationToken ct = default)
    {
        var lockKey = $"lock:{resourceKey}";
        var lockToken = Guid.NewGuid().ToString("N");
        var startTime = DateTime.UtcNow;

        if (_redis != null && _redis.IsConnected)
        {
            var db = _redis.GetDatabase();

            while (DateTime.UtcNow - startTime < waitTimeout && !ct.IsCancellationRequested)
            {
                bool acquired = await db.StringSetAsync(
                    lockKey,
                    lockToken,
                    expiry,
                    When.NotExists
                );

                if (acquired)
                {
                    _logger.LogDebug("[Distributed Lock] Acquired lock for {ResourceKey} (Token: {Token})", resourceKey, lockToken[..8]);
                    return new RedisLockReleaser(db, lockKey, lockToken, _logger);
                }

                await Task.Delay(50, ct); // Retry backoff
            }

            _logger.LogWarning("[Distributed Lock] Timed out waiting to acquire lock for {ResourceKey}", resourceKey);
            return null;
        }

        // In-Memory Fallback Semaphore
        var semaphore = _inMemoryLocks.GetOrAdd(lockKey, _ => new SemaphoreSlim(1, 1));
        bool inMemAcquired = await semaphore.WaitAsync(waitTimeout, ct);

        if (inMemAcquired)
        {
            return new InMemoryLockReleaser(semaphore, lockKey, _logger);
        }

        return null;
    }

    private sealed class RedisLockReleaser : IAsyncDisposable
    {
        private readonly IDatabase _db;
        private readonly string _lockKey;
        private readonly string _lockToken;
        private readonly ILogger _logger;
        private bool _disposed;

        public RedisLockReleaser(IDatabase db, string lockKey, string lockToken, ILogger logger)
        {
            _db = db;
            _lockKey = lockKey;
            _lockToken = lockToken;
            _logger = logger;
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;

            try
            {
                await _db.ScriptEvaluateAsync(
                    LuaScripts.ReleaseLock,
                    new RedisKey[] { _lockKey },
                    new RedisValue[] { _lockToken }
                );
                _logger.LogDebug("[Distributed Lock] Released lock for {LockKey}", _lockKey);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error releasing Redis lock {LockKey}", _lockKey);
            }
        }
    }

    private sealed class InMemoryLockReleaser : IAsyncDisposable
    {
        private readonly SemaphoreSlim _semaphore;
        private readonly string _lockKey;
        private readonly ILogger _logger;
        private bool _disposed;

        public InMemoryLockReleaser(SemaphoreSlim semaphore, string lockKey, ILogger logger)
        {
            _semaphore = semaphore;
            _lockKey = lockKey;
            _logger = logger;
        }

        public ValueTask DisposeAsync()
        {
            if (_disposed) return ValueTask.CompletedTask;
            _disposed = true;

            try
            {
                _semaphore.Release();
                _logger.LogDebug("[In-Memory Lock] Released semaphore for {LockKey}", _lockKey);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error releasing in-memory semaphore {LockKey}", _lockKey);
            }

            return ValueTask.CompletedTask;
        }
    }
}
