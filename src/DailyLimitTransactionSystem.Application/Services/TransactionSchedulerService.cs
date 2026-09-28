using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using Microsoft.Extensions.Logging;

namespace DailyLimitTransactionSystem.Application.Services;

public class TransactionSchedulerService
{
    private readonly IMessagePublisher _publisher;
    private readonly ITransactionRepository _repository;
    private readonly ILogger<TransactionSchedulerService> _logger;

    public TransactionSchedulerService(
        IMessagePublisher publisher,
        ITransactionRepository repository,
        ILogger<TransactionSchedulerService> logger)
    {
        _publisher = publisher;
        _repository = repository;
        _logger = logger;
    }

    /// <summary>
    /// Schedules a new transaction for a target date and time.
    /// </summary>
    public async Task<Transaction> ScheduleTransactionAsync(
        string userId,
        decimal amount,
        DateTime scheduledExecutionTime,
        string? description = null,
        CancellationToken ct = default)
    {
        var targetDate = DateOnly.FromDateTime(scheduledExecutionTime);
        var transaction = new Transaction
        {
            UserId = userId,
            Amount = amount,
            ScheduledExecutionTime = scheduledExecutionTime,
            TargetDate = targetDate,
            Description = description ?? $"Scheduled transaction of ${amount:N2}",
            Status = TransactionStatus.Scheduled
        };

        await _repository.SaveAsync(transaction, ct);
        await _publisher.PublishTransactionScheduledAsync(transaction, ct);

        _logger.LogInformation(
            "[Scheduler] Transaction scheduled: Id={TxId}, User={UserId}, Amount=${Amount:N2}, Scheduled={Time:yyyy-MM-dd HH:mm:ss}",
            transaction.Id[..8], userId, amount, scheduledExecutionTime
        );

        return transaction;
    }
}
