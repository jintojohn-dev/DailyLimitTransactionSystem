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
}
