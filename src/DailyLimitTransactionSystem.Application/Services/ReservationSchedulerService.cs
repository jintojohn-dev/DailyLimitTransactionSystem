using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using Microsoft.Extensions.Logging;

namespace DailyLimitTransactionSystem.Application.Services;

public class ReservationSchedulerService
{
    private readonly IDailyLimitService _dailyLimitService;
    private readonly IMessagePublisher _publisher;
    private readonly ITransactionRepository _repository;
    private readonly ILogger<ReservationSchedulerService> _logger;

    public ReservationSchedulerService(
        IDailyLimitService dailyLimitService,
        IMessagePublisher publisher,
        ITransactionRepository repository,
        ILogger<ReservationSchedulerService> logger)
    {
        _dailyLimitService = dailyLimitService;
        _publisher = publisher;
        _repository = repository;
        _logger = logger;
    }

    /// <summary>
    /// Strategy B: Pre-allocates/reserves daily quota at schedule time.
    /// Rejects early if user tries to schedule transactions exceeding the daily limit.
    /// </summary>
    public async Task<(bool Success, Transaction? Transaction, string Message)> ScheduleWithReservationAsync(
        string userId,
        decimal amount,
        DateTime scheduledExecutionTime,
        decimal dailyLimit = 3000.00m,
        string? description = null,
        CancellationToken ct = default)
    {
        var targetDate = DateOnly.FromDateTime(scheduledExecutionTime);

        // Pre-reserve quota atomically in Redis
        var reservationResult = await _dailyLimitService.TryDeductDailyLimitAsync(
            userId,
            targetDate,
            amount,
            dailyLimit,
            ttl: TimeSpan.FromDays(7),
            ct: ct
        );

        if (!reservationResult.IsAllowed)
        {
            _logger.LogWarning(
                "❌ [Reservation Denied] User {UserId} cannot schedule ${Amount:N2} for {Date:yyyy-MM-dd}. Current Reserved: ${Reserved:N2}, Max: ${Limit:N2}, Remaining: ${Remaining:N2}",
                userId, amount, targetDate, reservationResult.CurrentTotalSpent, dailyLimit, reservationResult.RemainingLimit
            );

            return (false, null, $"Daily limit for {targetDate:yyyy-MM-dd} is ${dailyLimit:N2}. Already reserved: ${reservationResult.CurrentTotalSpent:N2}. Cannot schedule ${amount:N2}. Remaining quota: ${reservationResult.RemainingLimit:N2}");
        }

        var transaction = new Transaction
        {
            UserId = userId,
            Amount = amount,
            ScheduledExecutionTime = scheduledExecutionTime,
            TargetDate = targetDate,
            Description = description ?? $"Pre-reserved transaction of ${amount:N2}",
            Status = TransactionStatus.Scheduled
        };

        await _repository.SaveAsync(transaction, ct);
        await _publisher.PublishTransactionScheduledAsync(transaction, ct);

        _logger.LogInformation(
            "✅ [Reservation Approved] Scheduled Tx {TxId} of ${Amount:N2} for user {UserId}. Reserved Total: ${Total:N2} / ${Limit:N2}",
            transaction.Id[..8], amount, userId, reservationResult.CurrentTotalSpent, dailyLimit
        );

        return (true, transaction, $"Scheduled successfully. Quota reserved. Remaining limit for {targetDate:yyyy-MM-dd}: ${reservationResult.RemainingLimit:N2}");
    }
}
