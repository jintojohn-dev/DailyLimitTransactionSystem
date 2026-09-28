namespace DailyLimitTransactionSystem.Core.Models;

public record DailyLimitPolicy
{
    public required string UserId { get; init; }
    public decimal DailyLimit { get; init; } = 3000.00m;
    public string Currency { get; init; } = "USD";
}

public class Transaction
{
    public string Id { get; set; } = Guid.NewGuid().ToString("N");
    public required string UserId { get; set; }
    public decimal Amount { get; set; }
    public DateTime ScheduledExecutionTime { get; set; }
    public DateOnly TargetDate { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
    public TransactionStatus Status { get; set; } = TransactionStatus.Scheduled;
    public RejectionReason RejectionReason { get; set; } = RejectionReason.None;
    public DateTime? ProcessedAt { get; set; }
    public string? Description { get; set; }
    public string? FailureDetails { get; set; }

    public override string ToString()
    {
        return $"[Tx {Id[..8]} | User: {UserId} | Amt: ${Amount:N2} | Scheduled: {ScheduledExecutionTime:HH:mm:ss} | Status: {Status}]";
    }
}
