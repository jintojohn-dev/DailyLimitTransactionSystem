namespace DailyLimitTransactionSystem.Core.Models;

public enum TransactionStatus
{
    Scheduled,
    Processing,
    Succeeded,
    Rejected,
    Failed,
    Cancelled
}

public enum RejectionReason
{
    None,
    DailyLimitExceeded,
    InsufficientFunds,
    DuplicateTransaction,
    UserLocked,
    SystemError
}
