using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using Microsoft.Extensions.Logging;

namespace DailyLimitTransactionSystem.Application.Services;

public class TransactionExecutionProcessor
{
    private readonly IDailyLimitService _dailyLimitService;
    private readonly IDistributedLockService _lockService;
    private readonly ITransactionRepository _transactionRepository;
    private readonly ILogger<TransactionExecutionProcessor> _logger;

    public TransactionExecutionProcessor(
        IDailyLimitService dailyLimitService,
        IDistributedLockService lockService,
        ITransactionRepository transactionRepository,
        ILogger<TransactionExecutionProcessor> logger)
    {
        _dailyLimitService = dailyLimitService;
        _lockService = lockService;
        _transactionRepository = transactionRepository;
        _logger = logger;
    }

    /// <summary>
    /// Core processing pipeline using Distributed Locking with PreProcess and Process steps.
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

        // 1. Distributed Mutex Lock per (User + Date)
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
            return TransactionResult.Rejected(
                transaction.Id,
                transaction.UserId,
                transaction.Amount,
                await _dailyLimitService.GetCurrentDailySpendAsync(transaction.UserId, transaction.TargetDate, ct),
                dailyLimit,
                RejectionReason.UserLocked,
                "Could not acquire user distributed lock. Please retry."
            );
        }

        try
        {
            if (string.IsNullOrWhiteSpace(transaction.ParticipantReferenceNumber))
            {
                await CreateTransferAsync(transaction, ct);
            }
            else if (string.IsNullOrWhiteSpace(transaction.TransferReferenceNumber))
            {
                await CompleteTransferAsync(transaction, ct);
            }

            else
            {

                _logger.LogInformation(
                    "✅ [SUCCESS] Tx {TxId} SUCCEEDED! Transferred: ${Amount:N2}, Total Day Spend: ${DailyLimit:N2}",
                    transaction.Id[..8], transaction.Amount, dailyLimit
                );
            }
            // 2. Pre-process: validate, enrich, and prepare the transfer


            // 3. Process: execute the transfer (dummy bank dispatch)



 


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
            return successResult;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unhandled exception processing Tx {TxId}", transaction.Id);
            throw;
        }
    }

    /// <summary>
    /// Dummy pre-processing step: validates and enriches the transfer before execution.
    /// In a real system this would perform AML/fraud checks, recipient validation, etc.
    /// </summary>
    private async Task CompleteTransferAsync(Transaction transaction, CancellationToken ct)
    {
        _logger.LogInformation(
            "[PreProcess] Validating and enriching Tx {TxId} | User: {UserId} | Amount: ${Amount:N2}",
            transaction.Id[..8], transaction.UserId, transaction.Amount
        );

        // Simulate async pre-processing work (e.g. compliance checks, data enrichment)
        await Task.Delay(10, ct);

        transaction.Status = TransactionStatus.Processing;
        _logger.LogInformation("[PreProcess] Tx {TxId} pre-processing complete.", transaction.Id[..8]);
    }

    /// <summary>
    /// Dummy transfer processing step: dispatches the transfer to the bank/payment gateway.
    /// In a real system this would call downstream APIs for fund transfer.
    /// </summary>
    private async Task<bool> CreateTransferAsync(Transaction transaction, CancellationToken ct)
    {
        _logger.LogInformation(
            "[ProcessTransfer] Dispatching Tx {TxId} to payment gateway | Amount: ${Amount:N2}",
            transaction.Id[..8], transaction.Amount
        );

        // Simulate async bank dispatch (e.g. API call to payment processor)
        await Task.Delay(10, ct);

        _logger.LogInformation("[ProcessTransfer] Tx {TxId} bank dispatch successful.", transaction.Id[..8]);
        return true;
    }
}

