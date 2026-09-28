using DailyLimitTransactionSystem.Core.Models;

namespace DailyLimitTransactionSystem.Core.Contracts;

public record ExecuteTransactionCommand
{
    public required string TransactionId { get; init; }
    public required string UserId { get; init; }
    public required decimal Amount { get; init; }
    public required DateTime ScheduledExecutionTime { get; init; }
    public required DateOnly TargetDate { get; init; }
    public string? Description { get; init; }
}

public record TransactionCompletedEvent
{
    public required string TransactionId { get; init; }
    public required string UserId { get; init; }
    public required decimal Amount { get; init; }
    public required decimal CurrentTotalSpent { get; init; }
    public required decimal DailyLimit { get; init; }
    public required decimal RemainingLimit { get; init; }
    public required bool IsSuccess { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

public record TransactionRejectedEvent
{
    public required string TransactionId { get; init; }
    public required string UserId { get; init; }
    public required decimal Amount { get; init; }
    public required decimal CurrentTotalSpent { get; init; }
    public required decimal DailyLimit { get; init; }
    public required decimal RemainingLimit { get; init; }
    public required RejectionReason Reason { get; init; }
    public required string Message { get; init; }
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}
