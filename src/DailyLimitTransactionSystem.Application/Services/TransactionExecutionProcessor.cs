using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using Microsoft.Extensions.Logging;

namespace DailyLimitTransactionSystem.Application.Services;

public class TransactionExecutionProcessor
{
    private readonly IDailyLimitService _dailyLimitService;
    private readonly IDistributedLockService _lockService;
    private readonly IIdempotencyService _idempotencyService;
    private readonly ITransactionRepository _transactionRepository;
    private readonly ILogger<TransactionExecutionProcessor> _logger;

    public TransactionExecutionProcessor(
        IDailyLimitService dailyLimitService,
        IDistributedLockService lockService,
        IIdempotencyService idempotencyService,
        ITransactionRepository transactionRepository,
        ILogger<TransactionExecutionProcessor> logger)
    {
        _dailyLimitService = dailyLimitService;
        _lockService = lockService;
        _idempotencyService = idempotencyService;
        _transactionRepository = transactionRepository;
        _logger = logger;
    }

    /// <summary>
    /// Core processing pipeline combining Distributed Locking, Idempotency, and Atomic Redis Lua verification.
    /// Returns a TransactionResult — event publishing is the caller's responsibility (e.g. ProcessTransactionConsumer).
    /// </summary>
    public async Task<TransactionResult> ProcessTransactionAsync(
        Transaction transaction,
        decimal dailyLimit = 3000.00m,
        CancellationToken ct = default)
    {
        _logger.LogInformation(
            "===> [Processor] Processing Tx {TxId} | User: {UserId} | Amount: ${Amount:N2} | Scheduled Time: {ScheduledTime:HH:mm:ss}",
            transaction.Id[..8], transaction.UserId, transaction.Amount, transaction.ScheduledExecutionTime
        );

        // 1. Idempotency Check
        var isNewExecution = await _idempotencyService.TryAcquireExecutionAsync(
            transaction.Id,
            TimeSpan.FromMinutes(30),
            ct
        );

        if (!isNewExecution)
        {
            var existingResult = await _idempotencyService.GetCompletedResultAsync(transaction.Id, ct);
            if (existingResult != null)
            {
                _logger.LogWarning("[Idempotency] Tx {TxId} was already completed. Returning cached result.", transaction.Id[..8]);
                return existingResult;
            }

            _logger.LogWarning("[Idempotency] Tx {TxId} is already in-flight by another worker. Skipping.", transaction.Id[..8]);
            return TransactionResult.Rejected(
                transaction.Id,
                transaction.UserId,
                transaction.Amount,
                await _dailyLimitService.GetCurrentDailySpendAsync(transaction.UserId, transaction.TargetDate, ct),
                dailyLimit,
                RejectionReason.DuplicateTransaction,
                "Transaction is currently being processed concurrently."
            );
        }

        // 2. Distributed Mutex Lock per (User + Date)
        // This ensures transactions for the same user on the same date are evaluated sequentially even across workers.
        var lockResourceKey = $"user:{transaction.UserId}:{transaction.TargetDate:yyyy-MM-dd}";
        await using var lockHandle = await _lockService.AcquireLockAsync(
            lockResourceKey,
            expiry: TimeSpan.FromSeconds(10),
            waitTimeout: TimeSpan.FromSeconds(5),
            ct
        );

        if (lockHandle == null)
        {
            _logger.LogError("[Distributed Lock] Could not acquire lock for {ResourceKey}. System busy.", lockResourceKey);
            var result = TransactionResult.Rejected(
                transaction.Id,
                transaction.UserId,
                transaction.Amount,
                await _dailyLimitService.GetCurrentDailySpendAsync(transaction.UserId, transaction.TargetDate, ct),
                dailyLimit,
                RejectionReason.UserLocked,
                "Could not acquire user distributed lock. Please retry."
            );
            await _idempotencyService.MarkCompletedAsync(transaction.Id, result, TimeSpan.FromHours(1), ct);
            return result;
        }

        try
        {
            // 3. Atomic Redis Lua Script: Check & Deduct Limit
            var limitResult = await _dailyLimitService.TryDeductDailyLimitAsync(
                transaction.UserId,
                transaction.TargetDate,
                transaction.Amount,
                dailyLimit,
                ttl: TimeSpan.FromHours(48),
                ct: ct
            );

            if (!limitResult.IsAllowed)
            {
                // Daily limit exceeded!
                _logger.LogWarning(
                    "❌ [REJECTED] Tx {TxId} REJECTED! Requested: ${Amount:N2}, Current Spent: ${CurrentSpent:N2}, Limit: ${DailyLimit:N2}, Remaining Limit: ${Remaining:N2}",
                    transaction.Id[..8], transaction.Amount, limitResult.CurrentTotalSpent, dailyLimit, limitResult.RemainingLimit
                );

                transaction.Status = TransactionStatus.Rejected;
                transaction.RejectionReason = RejectionReason.DailyLimitExceeded;
                transaction.FailureDetails = limitResult.Reason;
                transaction.ProcessedAt = DateTime.UtcNow;

                var result = TransactionResult.Rejected(
                    transaction.Id,
                    transaction.UserId,
                    transaction.Amount,
                    limitResult.CurrentTotalSpent,
                    dailyLimit,
                    RejectionReason.DailyLimitExceeded,
                    $"Daily limit of ${dailyLimit:N2} exceeded. Current spend: ${limitResult.CurrentTotalSpent:N2}, Remaining: ${limitResult.RemainingLimit:N2}"
                );

                await _transactionRepository.SaveAsync(transaction, ct);
                await _idempotencyService.MarkCompletedAsync(transaction.Id, result, TimeSpan.FromHours(24), ct);
                return result;
            }

            // 4. Simulate Downstream Payment Gateway / Bank Dispatch
            bool paymentSuccess = await DispatchPaymentToBankAsync(transaction, ct);

            if (!paymentSuccess)
            {
                // Downstream failed: Roll back daily limit in Redis atomically
                _logger.LogError("Bank dispatch failed for Tx {TxId}. Rolling back daily spend...", transaction.Id[..8]);
                await _dailyLimitService.RollbackDailyLimitAsync(transaction.UserId, transaction.TargetDate, transaction.Amount, ct);

                transaction.Status = TransactionStatus.Failed;
                transaction.RejectionReason = RejectionReason.SystemError;
                transaction.FailureDetails = "Downstream bank transfer rejected.";
                transaction.ProcessedAt = DateTime.UtcNow;

                var currentSpend = await _dailyLimitService.GetCurrentDailySpendAsync(transaction.UserId, transaction.TargetDate, ct);
                var result = TransactionResult.Rejected(
                    transaction.Id,
                    transaction.UserId,
                    transaction.Amount,
                    currentSpend,
                    dailyLimit,
                    RejectionReason.SystemError,
                    "Payment gateway error. Daily limit rolled back."
                );

                await _transactionRepository.SaveAsync(transaction, ct);
                await _idempotencyService.MarkCompletedAsync(transaction.Id, result, TimeSpan.FromHours(24), ct);
                return result;
            }

            // 5. Success!
            _logger.LogInformation(
                "✅ [SUCCESS] Tx {TxId} SUCCEEDED! Transferred: ${Amount:N2}, Total Day Spend: ${CurrentSpent:N2} / ${DailyLimit:N2}, Remaining: ${Remaining:N2}",
                transaction.Id[..8], transaction.Amount, limitResult.CurrentTotalSpent, dailyLimit, limitResult.RemainingLimit
            );

            transaction.Status = TransactionStatus.Succeeded;
            transaction.ProcessedAt = DateTime.UtcNow;

            var successResult = TransactionResult.Succeeded(
                transaction.Id,
                transaction.UserId,
                transaction.Amount,
                limitResult.CurrentTotalSpent,
                dailyLimit
            );

            await _transactionRepository.SaveAsync(transaction, ct);
            await _idempotencyService.MarkCompletedAsync(transaction.Id, successResult, TimeSpan.FromHours(24), ct);
            return successResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled exception processing Tx {TxId}", transaction.Id);
            throw;
        }
    }

    private Task<bool> DispatchPaymentToBankAsync(Transaction transaction, CancellationToken ct)
    {
        // Simulated bank / payment gateway invocation
        return Task.FromResult(true);
    }
}
