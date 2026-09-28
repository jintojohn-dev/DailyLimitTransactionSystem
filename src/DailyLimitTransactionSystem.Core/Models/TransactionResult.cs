namespace DailyLimitTransactionSystem.Core.Models;

public record DailyLimitCheckResult
{
    public required bool IsAllowed { get; init; }
    public required decimal AmountRequested { get; init; }
    public required decimal CurrentTotalSpent { get; init; }
    public required decimal DailyLimit { get; init; }
    public required decimal RemainingLimit { get; init; }
    public string? Reason { get; init; }
}

public record TransactionResult
{
    public required bool IsSuccess { get; init; }
    public required string TransactionId { get; init; }
    public required string UserId { get; init; }
    public required decimal Amount { get; init; }
    public required decimal DailyLimit { get; init; }
    public required decimal CurrentTotalSpent { get; init; }
    public required decimal RemainingLimit { get; init; }
    public RejectionReason RejectionReason { get; init; } = RejectionReason.None;
    public string Message { get; init; } = string.Empty;
    public DateTime ProcessedAt { get; init; } = DateTime.UtcNow;
    public string? ExecutionThreadId { get; init; }

    public static TransactionResult Succeeded(
        string transactionId,
        string userId,
        decimal amount,
        decimal currentTotalSpent,
        decimal dailyLimit) =>
        new()
        {
            IsSuccess = true,
            TransactionId = transactionId,
            UserId = userId,
            Amount = amount,
            DailyLimit = dailyLimit,
            CurrentTotalSpent = currentTotalSpent,
            RemainingLimit = Math.Max(0, dailyLimit - currentTotalSpent),
            Message = $"Transaction of ${amount:N2} processed successfully. Remaining limit: ${Math.Max(0, dailyLimit - currentTotalSpent):N2}"
        };

    public static TransactionResult Rejected(
        string transactionId,
        string userId,
        decimal amount,
        decimal currentTotalSpent,
        decimal dailyLimit,
        RejectionReason reason,
        string message) =>
        new()
        {
            IsSuccess = false,
            TransactionId = transactionId,
            UserId = userId,
            Amount = amount,
            DailyLimit = dailyLimit,
            CurrentTotalSpent = currentTotalSpent,
            RemainingLimit = Math.Max(0, dailyLimit - currentTotalSpent),
            RejectionReason = reason,
            Message = message
        };
}
