using DailyLimitTransactionSystem.Application.Services;
using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using DailyLimitTransactionSystem.Infrastructure.Redis;
using DailyLimitTransactionSystem.Infrastructure.Repositories;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;

namespace DailyLimitTransactionSystem.Tests;

public class ConcurrencyAndLimitTests
{
    private readonly IDailyLimitService _dailyLimitService;
    private readonly IDistributedLockService _lockService;
    private readonly ITransactionRepository _repository;
    private readonly TransactionExecutionProcessor _processor;

    public ConcurrencyAndLimitTests()
    {
        _dailyLimitService = new RedisDailyLimitService(NullLogger<RedisDailyLimitService>.Instance);
        _lockService = new RedisDistributedLockService(NullLogger<RedisDistributedLockService>.Instance);
        _repository = new InMemoryTransactionRepository();

        _processor = new TransactionExecutionProcessor(
            _dailyLimitService,
            _lockService,
            _repository,
            NullLogger<TransactionExecutionProcessor>.Instance
        );
    }

    [Fact]
    public async Task IrregularOrder_T2_T1_T3_StrictlyEnforces_3000_Limit()
    {
        // Scenario:
        // User daily limit: $3000
        // T1 = $1500 (09:00 AM), T2 = $2000 (12:00 PM), T3 = $1000 (03:00 PM)
        // Irregular arrival order: T2 arrives first, then T1, then T3

        var userId = $"user_{Guid.NewGuid():N}";
        var targetDate = new DateOnly(2026, 9, 24);
        decimal dailyLimit = 3000.00m;

        var t1 = new Transaction
        {
            Id = "T1_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 1500.00m,
            ScheduledExecutionTime = new DateTime(2026, 9, 24, 9, 0, 0),
            TargetDate = targetDate,
            Description = "T1: Scheduled for 09:00 AM ($1500)"
        };

        var t2 = new Transaction
        {
            Id = "T2_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 2000.00m,
            ScheduledExecutionTime = new DateTime(2026, 9, 24, 12, 0, 0),
            TargetDate = targetDate,
            Description = "T2: Scheduled for 12:00 PM ($2000)"
        };

        var t3 = new Transaction
        {
            Id = "T3_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 1000.00m,
            ScheduledExecutionTime = new DateTime(2026, 9, 24, 15, 0, 0),
            TargetDate = targetDate,
            Description = "T3: Scheduled for 03:00 PM ($1000)"
        };

        // 1. T2 ($2000) arrives first
        var resultT2 = await _processor.ProcessTransactionAsync(t2, dailyLimit);
        resultT2.IsSuccess.Should().BeTrue("T2 ($2000) <= $3000 limit");
        resultT2.CurrentTotalSpent.Should().Be(2000.00m);
        resultT2.RemainingLimit.Should().Be(1000.00m);

        // 2. T1 ($1500) arrives second
        var resultT1 = await _processor.ProcessTransactionAsync(t1, dailyLimit);
        resultT1.IsSuccess.Should().BeFalse("T1 ($1500) + spent ($2000) = $3500 > $3000 limit");
        resultT1.RejectionReason.Should().Be(RejectionReason.DailyLimitExceeded);
        resultT1.CurrentTotalSpent.Should().Be(2000.00m, "Spend must NOT increase on rejection");
        resultT1.RemainingLimit.Should().Be(1000.00m);

        // 3. T3 ($1000) arrives third
        var resultT3 = await _processor.ProcessTransactionAsync(t3, dailyLimit);
        resultT3.IsSuccess.Should().BeTrue("T3 ($1000) + spent ($2000) = $3000 <= $3000 limit");
        resultT3.CurrentTotalSpent.Should().Be(3000.00m);
        resultT3.RemainingLimit.Should().Be(0.00m);

        // Final total spend verification
        var finalSpend = await _dailyLimitService.GetCurrentDailySpendAsync(userId, targetDate);
        finalSpend.Should().Be(3000.00m, "Total money dispatched must never exceed $3000");
    }

    [Fact]
    public async Task HighConcurrency_ParallelExecution_NeverExceeds_DailyLimit()
    {
        var userId = $"user_concurrent_{Guid.NewGuid():N}";
        var targetDate = new DateOnly(2026, 9, 24);
        decimal dailyLimit = 3000.00m;

        // 20 concurrent transactions of $500 each = $10,000 attempted
        var transactions = Enumerable.Range(1, 20).Select(i => new Transaction
        {
            Id = $"Tx_Concurrent_{i}_{Guid.NewGuid():N}",
            UserId = userId,
            Amount = 500.00m,
            ScheduledExecutionTime = DateTime.UtcNow.AddMinutes(i),
            TargetDate = targetDate,
            Description = $"Concurrent Tx {i}"
        }).ToList();

        // Launch all 20 in parallel
        var tasks = transactions.Select(tx => Task.Run(() => _processor.ProcessTransactionAsync(tx, dailyLimit)));
        var results = await Task.WhenAll(tasks);

        var successfulTx = results.Where(r => r.IsSuccess).ToList();
        var rejectedTx = results.Where(r => !r.IsSuccess).ToList();

        successfulTx.Count.Should().Be(6, "Exactly 6 transactions of $500 can succeed within $3000");
        rejectedTx.Count.Should().Be(14, "The remaining 14 transactions must be rejected");

        var finalSpend = await _dailyLimitService.GetCurrentDailySpendAsync(userId, targetDate);
        finalSpend.Should().Be(3000.00m);
    }



    [Fact]
    public async Task Bug1_DailyLimitEnforced_SecondTransferRejected()
    {
        var userId = $"user_bug1_{Guid.NewGuid():N}";
        var targetDate = new DateOnly(2026, 9, 30);
        decimal dailyLimit = 3000.00m;

        var transfer1 = new Transaction
        {
            Id = "Bug1_Transfer1_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 2200.00m,
            ScheduledExecutionTime = new DateTime(2026, 9, 30, 9, 0, 0),
            TargetDate = targetDate,
            Description = "Scheduled e-Transfer #1: $2,200"
        };

        var transfer2 = new Transaction
        {
            Id = "Bug1_Transfer2_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 1500.00m,
            ScheduledExecutionTime = new DateTime(2026, 9, 30, 10, 0, 0),
            TargetDate = targetDate,
            Description = "Scheduled e-Transfer #2: $1,500"
        };

        // Process Transfer #1 ($2,200)
        var result1 = await _processor.ProcessTransactionAsync(transfer1, dailyLimit);
        result1.IsSuccess.Should().BeTrue("$2,200 ≤ $3,000 daily limit");
        result1.CurrentTotalSpent.Should().Be(2200.00m);

        // Process Transfer #2 ($1,500) — should be REJECTED
        var result2 = await _processor.ProcessTransactionAsync(transfer2, dailyLimit);
        result2.IsSuccess.Should().BeFalse("$2,200 + $1,500 = $3,700 > $3,000 daily limit");
        result2.RejectionReason.Should().Be(RejectionReason.DailyLimitExceeded);

        // Verify only $2,200 was actually spent
        var finalSpend = await _dailyLimitService.GetCurrentDailySpendAsync(userId, targetDate);
        finalSpend.Should().Be(2200.00m, "Only Transfer #1 should have been processed");
    }
}
