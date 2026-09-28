using System.Collections.Concurrent;
using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;

namespace DailyLimitTransactionSystem.Infrastructure.Repositories;

public class InMemoryTransactionRepository : ITransactionRepository
{
    private readonly ConcurrentDictionary<string, Transaction> _transactions = new();

    public Task SaveAsync(Transaction transaction, CancellationToken ct = default)
    {
        _transactions[transaction.Id] = transaction;
        return Task.CompletedTask;
    }

    public Task<Transaction?> GetByIdAsync(string id, CancellationToken ct = default)
    {
        _transactions.TryGetValue(id, out var tx);
        return Task.FromResult(tx);
    }

    public Task<IReadOnlyList<Transaction>> GetByUserIdAndDateAsync(string userId, DateOnly date, CancellationToken ct = default)
    {
        var list = _transactions.Values
            .Where(t => t.UserId == userId && t.TargetDate == date)
            .OrderBy(t => t.ScheduledExecutionTime)
            .ToList();

        return Task.FromResult<IReadOnlyList<Transaction>>(list);
    }

    public Task<IReadOnlyList<Transaction>> GetAllAsync(CancellationToken ct = default)
    {
        var list = _transactions.Values.OrderBy(t => t.ScheduledExecutionTime).ToList();
        return Task.FromResult<IReadOnlyList<Transaction>>(list);
    }
}
