using DailyLimitTransactionSystem.Application.Services;
using DailyLimitTransactionSystem.Core.Contracts;
using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using DailyLimitTransactionSystem.Infrastructure.MassTransit;
using DailyLimitTransactionSystem.Infrastructure.Redis;
using DailyLimitTransactionSystem.Infrastructure.Repositories;
using FluentAssertions;
using MassTransit;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace DailyLimitTransactionSystem.Tests;

public class ConcurrencyAndLimitTests
{
    private readonly IDailyLimitService _dailyLimitService;
    private readonly IDistributedLockService _lockService;
    private readonly IIdempotencyService _idempotencyService;
    private readonly ITransactionRepository _repository;
    private readonly IMessagePublisher _publisher;
    private readonly TransactionExecutionProcessor _processor;
    private readonly Mock<IPublishEndpoint> _publishEndpointMock;

    public ConcurrencyAndLimitTests()
    {
        _dailyLimitService = new RedisDailyLimitService(NullLogger<RedisDailyLimitService>.Instance);
        _lockService = new RedisDistributedLockService(NullLogger<RedisDistributedLockService>.Instance);
        _idempotencyService = new RedisIdempotencyService(NullLogger<RedisIdempotencyService>.Instance);
        _repository = new InMemoryTransactionRepository();

        _publishEndpointMock = new Mock<IPublishEndpoint>();
        _publisher = new MassTransitMessagePublisher(
            _publishEndpointMock.Object,
            NullLogger<MassTransitMessagePublisher>.Instance
        );

        _processor = new TransactionExecutionProcessor(
            _dailyLimitService,
            _lockService,
            _idempotencyService,
            _repository,
            _publisher,
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
    public async Task Idempotency_DuplicateMessage_DoesNotDoubleDeduct()
    {
        var userId = $"user_idempotent_{Guid.NewGuid():N}";
        var targetDate = new DateOnly(2026, 9, 24);
        decimal dailyLimit = 3000.00m;

        var tx = new Transaction
        {
            Id = "SameTxId_MassTransit_12345",
            UserId = userId,
            Amount = 1500.00m,
            ScheduledExecutionTime = DateTime.UtcNow,
            TargetDate = targetDate
        };

        // First delivery
        var res1 = await _processor.ProcessTransactionAsync(tx, dailyLimit);
        res1.IsSuccess.Should().BeTrue();
        res1.CurrentTotalSpent.Should().Be(1500.00m);

        // Duplicate delivery (e.g. MassTransit / Azure Service Bus redelivery)
        var res2 = await _processor.ProcessTransactionAsync(tx, dailyLimit);
        res2.IsSuccess.Should().BeTrue();
        res2.CurrentTotalSpent.Should().Be(1500.00m);

        var finalSpend = await _dailyLimitService.GetCurrentDailySpendAsync(userId, targetDate);
        finalSpend.Should().Be(1500.00m, "Duplicate delivery must not increment spend");
    }

    [Fact]
    public async Task ReservationPattern_RejectsEarlyAtScheduleTime()
    {
        var userId = $"user_reserve_{Guid.NewGuid():N}";
        var targetDate = DateTime.UtcNow.Date.AddDays(1);
        decimal dailyLimit = 3000.00m;

        var reservationService = new ReservationSchedulerService(
            _dailyLimitService,
            _publisher,
            _repository,
            NullLogger<ReservationSchedulerService>.Instance
        );

        // Schedule T1: $1500
        var (t1Success, _, _) = await reservationService.ScheduleWithReservationAsync(
            userId, 1500.00m, targetDate.AddHours(9), dailyLimit
        );
        t1Success.Should().BeTrue();

        // Schedule T2: $2000 -> Should fail immediately at scheduling time!
        var (t2Success, _, t2Msg) = await reservationService.ScheduleWithReservationAsync(
            userId, 2000.00m, targetDate.AddHours(12), dailyLimit
        );
        t2Success.Should().BeFalse("1500 + 2000 = 3500 > 3000");
        t2Msg.Should().Contain("Daily limit");

        // Schedule T3: $1000 -> Should succeed (1500 + 1000 = 2500 <= 3000)
        var (t3Success, _, _) = await reservationService.ScheduleWithReservationAsync(
            userId, 1000.00m, targetDate.AddHours(15), dailyLimit
        );
        t3Success.Should().BeTrue();

        var totalReserved = await _dailyLimitService.GetCurrentDailySpendAsync(userId, DateOnly.FromDateTime(targetDate));
        totalReserved.Should().Be(2500.00m);
    }

    /// <summary>
    /// Bug #2 Regression Test: "Scheduled e-Transfers not processed in creation order"
    /// 
    /// Scenario from QA (qa282-ob1.qa.libro.ca):
    ///   - Account daily e-Transfer limit: $3,000
    ///   - T1 ($1,500) created FIRST at 10:00 AM for tomorrow
    ///   - T2 ($1,000) created SECOND at 10:05 AM for tomorrow
    ///   - T3 ($800) created THIRD at 10:10 AM for tomorrow
    ///   - Total: $3,300 > $3,000 limit
    /// 
    /// Bug behavior (BEFORE fix):
    ///   T2 ($1,000) processed first → Success
    ///   T3 ($800) processed second → Success  
    ///   T1 ($1,500) processed third → FAILED ($1,000 + $800 + $1,500 = $3,300 > $3,000)
    ///   ❌ The earliest-created transfer T1 was rejected instead of the latest T3.
    /// 
    /// Expected behavior (AFTER fix):
    ///   T1 ($1,500) processed first → Success (created first, FIFO priority)
    ///   T2 ($1,000) processed second → Success ($1,500 + $1,000 = $2,500 ≤ $3,000)
    ///   T3 ($800) FAILS ($2,500 + $800 = $3,300 > $3,000)
    ///   ✅ The latest-created transfer T3 is rejected, not the earliest T1.
    /// </summary>
    [Fact]
    public async Task Bug2_CreationOrder_FIFO_EarliestCreatedTransferProcessedFirst()
    {
        var userId = $"user_bug2_{Guid.NewGuid():N}";
        var targetDate = new DateOnly(2026, 9, 30);
        var tomorrow = new DateTime(2026, 9, 30, 0, 0, 0, DateTimeKind.Utc);
        decimal dailyLimit = 3000.00m;

        // Create transactions with EXPLICIT creation timestamps to simulate creation order.
        // All 3 are scheduled for the same processing date (tomorrow) but created at different times.
        var t1 = new Transaction
        {
            Id = "Bug2_T1_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 1500.00m,
            ScheduledExecutionTime = tomorrow.AddHours(9),  // Scheduled for 09:00
            TargetDate = targetDate,
            CreatedAt = new DateTime(2026, 9, 29, 10, 0, 0, DateTimeKind.Utc), // Created FIRST at 10:00 AM
            Description = "T1: $1,500 (Created FIRST)"
        };

        var t2 = new Transaction
        {
            Id = "Bug2_T2_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 1000.00m,
            ScheduledExecutionTime = tomorrow.AddHours(9),  // Same scheduled time!
            TargetDate = targetDate,
            CreatedAt = new DateTime(2026, 9, 29, 10, 5, 0, DateTimeKind.Utc), // Created SECOND at 10:05 AM
            Description = "T2: $1,000 (Created SECOND)"
        };

        var t3 = new Transaction
        {
            Id = "Bug2_T3_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 800.00m,
            ScheduledExecutionTime = tomorrow.AddHours(9),  // Same scheduled time!
            TargetDate = targetDate,
            CreatedAt = new DateTime(2026, 9, 29, 10, 10, 0, DateTimeKind.Utc), // Created THIRD at 10:10 AM
            Description = "T3: $800 (Created THIRD)"
        };

        // Schedule all 3 into the RedisMessageScheduler (in any order — simulates real-world)
        var scheduler = new RedisMessageScheduler(NullLogger<RedisMessageScheduler>.Instance);

        // Deliberately schedule in WRONG order to prove FIFO fix works
        await scheduler.ScheduleAsync(t3); // T3 added first to sorted set
        await scheduler.ScheduleAsync(t1); // T1 added second
        await scheduler.ScheduleAsync(t2); // T2 added third

        // Poll for due transactions — should come back sorted by CreatedAt (T1, T2, T3)
        var dueTransactions = await scheduler.GetAndPollDueTransactionsAsync(tomorrow.AddHours(18));

        dueTransactions.Should().HaveCount(3);
        dueTransactions[0].Id.Should().Be(t1.Id, "T1 was created FIRST and must be dispatched first");
        dueTransactions[1].Id.Should().Be(t2.Id, "T2 was created SECOND and must be dispatched second");
        dueTransactions[2].Id.Should().Be(t3.Id, "T3 was created THIRD and must be dispatched last");

        // Now process them in creation-order FIFO sequence
        var resultT1 = await _processor.ProcessTransactionAsync(dueTransactions[0], dailyLimit);
        resultT1.IsSuccess.Should().BeTrue("T1 ($1,500) is first — $1,500 ≤ $3,000");
        resultT1.CurrentTotalSpent.Should().Be(1500.00m);

        var resultT2 = await _processor.ProcessTransactionAsync(dueTransactions[1], dailyLimit);
        resultT2.IsSuccess.Should().BeTrue("T2 ($1,000) is second — $1,500 + $1,000 = $2,500 ≤ $3,000");
        resultT2.CurrentTotalSpent.Should().Be(2500.00m);

        var resultT3 = await _processor.ProcessTransactionAsync(dueTransactions[2], dailyLimit);
        resultT3.IsSuccess.Should().BeFalse("T3 ($800) is third — $2,500 + $800 = $3,300 > $3,000");
        resultT3.RejectionReason.Should().Be(RejectionReason.DailyLimitExceeded);

        // Verify final spend
        var finalSpend = await _dailyLimitService.GetCurrentDailySpendAsync(userId, targetDate);
        finalSpend.Should().Be(2500.00m, "Only T1 ($1,500) + T2 ($1,000) should have been processed");
    }

    /// <summary>
    /// Bug #1 Regression Test: "Daily limit exceeded on scheduled e-Transfers"
    /// 
    /// Scenario from QA:
    ///   - Account daily e-Transfer limit: $3,000
    ///   - Scheduled e-Transfer #1: $2,200
    ///   - Scheduled e-Transfer #2: $1,500
    ///   - Combined: $3,700 > $3,000
    /// 
    /// Bug behavior: Both processed successfully ($3,700 sent).
    /// Expected behavior: First succeeds ($2,200), second is REJECTED ($2,200 + $1,500 = $3,700 > $3,000).
    /// </summary>
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
