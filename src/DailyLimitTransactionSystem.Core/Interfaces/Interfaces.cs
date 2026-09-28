using DailyLimitTransactionSystem.Core.Models;

namespace DailyLimitTransactionSystem.Core.Interfaces;

public interface IDailyLimitService
{
    /// <summary>
    /// Atomically verifies whether amount exceeds daily limit, and if valid, increments the user's daily spend.
    /// Executed via atomic Redis Lua script to prevent any race condition.
    /// </summary>
    Task<DailyLimitCheckResult> TryDeductDailyLimitAsync(
        string userId,
        DateOnly date,
        decimal amount,
        decimal dailyLimit,
        TimeSpan? ttl = null,
        CancellationToken ct = default);

    /// <summary>
    /// Gets current spent amount for the specified user and date.
    /// </summary>
    Task<decimal> GetCurrentDailySpendAsync(string userId, DateOnly date, CancellationToken ct = default);

    /// <summary>
    /// Atomically rolls back a spend (e.g. if downstream payment gateway fails).
    /// </summary>
    Task RollbackDailyLimitAsync(string userId, DateOnly date, decimal amount, CancellationToken ct = default);

    /// <summary>
    /// Resets daily limit spend for testing/administrative purposes.
    /// </summary>
    Task ResetDailyLimitAsync(string userId, DateOnly date, CancellationToken ct = default);
}

public interface IDistributedLockService
{
    /// <summary>
    /// Acquires a distributed lock for a specific resource key (e.g. "lock:user:123:2026-09-24").
    /// Returns an IAsyncDisposable handle that releases the lock on disposal.
    /// </summary>
    Task<IAsyncDisposable?> AcquireLockAsync(
        string resourceKey,
        TimeSpan expiry,
        TimeSpan waitTimeout,
        CancellationToken ct = default);
}

public interface IIdempotencyService
{
    Task<bool> TryAcquireExecutionAsync(string transactionId, TimeSpan expiry, CancellationToken ct = default);
    Task MarkCompletedAsync(string transactionId, TransactionResult result, TimeSpan expiry, CancellationToken ct = default);
    Task<TransactionResult?> GetCompletedResultAsync(string transactionId, CancellationToken ct = default);
}

public interface ITransactionRepository
{
    Task SaveAsync(Transaction transaction, CancellationToken ct = default);
    Task<Transaction?> GetByIdAsync(string id, CancellationToken ct = default);
    Task<IReadOnlyList<Transaction>> GetByUserIdAndDateAsync(string userId, DateOnly date, CancellationToken ct = default);
    Task<IReadOnlyList<Transaction>> GetAllAsync(CancellationToken ct = default);
}

public interface IMessagePublisher
{
    Task PublishTransactionScheduledAsync(Transaction transaction, CancellationToken ct = default);
    Task PublishTransactionCompletedAsync(TransactionResult result, CancellationToken ct = default);
}

public interface IMessageConsumer
{
    Task StartConsumingAsync(Func<Transaction, Task<TransactionResult>> handler, CancellationToken ct = default);
    Task StopConsumingAsync(CancellationToken ct = default);
}
