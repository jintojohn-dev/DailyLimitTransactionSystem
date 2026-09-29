using DailyLimitTransactionSystem.Infrastructure.Redis;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DailyLimitTransactionSystem.Tests;

public class DailyLimitServiceTests
{
    [Fact]
    public async Task TryDeduct_ThenRollback_Restores_Spend()
    {
        var logger = NullLogger<RedisDailyLimitService>.Instance;
        var svc = new RedisDailyLimitService(logger);

        var userId = $"test_user_{Guid.NewGuid():N}";
        var date = new DateOnly(2026, 10, 1);

        // initial spend should be zero
        var initial = await svc.GetCurrentDailySpendAsync(userId, date);
        initial.Should().Be(0m);

        // deduct 1000
        var res = await svc.TryDeductDailyLimitAsync(userId, date, 1000m, 3000m);
        res.IsAllowed.Should().BeTrue();
        res.CurrentTotalSpent.Should().Be(1000m);

        // rollback 1000
        await svc.RollbackDailyLimitAsync(userId, date, 1000m);

        var after = await svc.GetCurrentDailySpendAsync(userId, date);
        after.Should().Be(0m);
    }

    [Fact]
    public async Task TryDeduct_ExceedingLimit_IsRejected()
    {
        var svc = new RedisDailyLimitService(NullLogger<RedisDailyLimitService>.Instance);
        var userId = $"test_user_{Guid.NewGuid():N}";
        var date = new DateOnly(2026, 10, 2);

        var res1 = await svc.TryDeductDailyLimitAsync(userId, date, 2500m, 3000m);
        res1.IsAllowed.Should().BeTrue();

        var res2 = await svc.TryDeductDailyLimitAsync(userId, date, 1000m, 3000m);
        res2.IsAllowed.Should().BeFalse();
        res2.CurrentTotalSpent.Should().Be(2500m);
    }

    [Fact]
    public void ComputeEffectiveTtl_Uses_Buffer_When_Ttl_Not_Provided()
    {
        var buffer = TimeSpan.FromHours(5);
        var svc = new RedisDailyLimitService(NullLogger<RedisDailyLimitService>.Instance, null, buffer);

        var targetDate = DateOnly.FromDateTime(DateTime.UtcNow.AddDays(1));
        var computed = svc.ComputeEffectiveTtl(targetDate);

        // Expected = (start of next day UTC - now) + buffer
        var nextDayUtc = DateTime.SpecifyKind(targetDate.AddDays(1).ToDateTime(new TimeOnly(0, 0)), DateTimeKind.Utc);
        var expected = nextDayUtc - DateTime.UtcNow + buffer;
        if (expected < TimeSpan.FromMinutes(1)) expected = TimeSpan.FromMinutes(1);

        // Allow small timing difference
        (Math.Abs((computed - expected).TotalSeconds) <= 2).Should().BeTrue("Computed TTL should match expected formula within a small margin");
    }

    [Fact]
    public void ComputeEffectiveTtl_Respects_Provided_Ttl()
    {
        var svc = new RedisDailyLimitService(NullLogger<RedisDailyLimitService>.Instance, null, TimeSpan.FromHours(24));
        var targetDate = DateOnly.FromDateTime(DateTime.UtcNow.AddDays(3));
        var overrideTtl = TimeSpan.FromHours(2);

        var computed = svc.ComputeEffectiveTtl(targetDate, overrideTtl);
        computed.Should().Be(overrideTtl);
    }
}
