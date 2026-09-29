using DailyLimitTransactionSystem.Application.Services;
using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using DailyLimitTransactionSystem.Infrastructure.Redis;
using DailyLimitTransactionSystem.Infrastructure.Repositories;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace DailyLimitTransactionSystem.ConsoleApp;

public class Program
{
    public static async Task Main(string[] args)
    {
        Console.OutputEncoding = System.Text.Encoding.UTF8;
        Console.WriteLine("Running Schedule E-Transfer demo (Bug #1 reproduction)\n");

        // Load basic configuration (appsettings.json optional)
        var configuration = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

        var redisConnectionString = configuration["Redis:ConnectionString"] ?? "localhost:6379,abortConnect=false,connectTimeout=1000";
        decimal dailyLimit = configuration.GetValue<decimal>("TransactionLimits:DefaultDailyLimit", 3000.00m);

        // Setup DI
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddSimpleConsole());

        // Redis connection (optional)
        IConnectionMultiplexer? redis = null;
        try
        {
            var redisOptions = ConfigurationOptions.Parse(redisConnectionString);
            redis = ConnectionMultiplexer.Connect(redisOptions);
        }
        catch { /* ignore - use in-memory fallback */ }

        services.AddSingleton<IDailyLimitService>(sp => new RedisDailyLimitService(sp.GetRequiredService<ILogger<RedisDailyLimitService>>(), redis));
        services.AddSingleton<IDistributedLockService>(sp => new RedisDistributedLockService(sp.GetRequiredService<ILogger<RedisDistributedLockService>>(), redis));

        services.AddSingleton<ITransactionRepository, InMemoryTransactionRepository>();
        services.AddSingleton<TransactionExecutionProcessor>();

        var provider = services.BuildServiceProvider();

        // Run the single scenario: two scheduled transfers that together exceed daily limit
        await RunBug1ScenarioAsync(provider, dailyLimit);
    }

    private static async Task RunBug1ScenarioAsync(IServiceProvider provider, decimal dailyLimit)
    {
        var processor = provider.GetRequiredService<TransactionExecutionProcessor>();
        var limitService = provider.GetRequiredService<IDailyLimitService>();

        var userId = $"user_bug1_{Guid.NewGuid():N}";
        var targetDate = DateOnly.FromDateTime(DateTime.UtcNow.AddDays(1));

        await limitService.ResetDailyLimitAsync(userId, targetDate);

        var transfer1 = new Transaction
        {
            Id = "Bug1_Transfer1_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 2200.00m,
            ScheduledExecutionTime = DateTime.UtcNow.AddDays(1).AddHours(9),
            TargetDate = targetDate,
            Description = "Scheduled e-Transfer #1: $2,200"
        };

        var transfer2 = new Transaction
        {
            Id = "Bug1_Transfer2_" + Guid.NewGuid().ToString("N"),
            UserId = userId,
            Amount = 1500.00m,
            ScheduledExecutionTime = DateTime.UtcNow.AddDays(1).AddHours(10),
            TargetDate = targetDate,
            Description = "Scheduled e-Transfer #2: $1,500"
        };

        Console.WriteLine("Processing Transfer #1 ($2,200) ...");
        var result1 = await processor.ProcessTransactionAsync(transfer1, dailyLimit);
        PrintResult(result1);

        Console.WriteLine("\nProcessing Transfer #2 ($1,500) ...");
        var result2 = await processor.ProcessTransactionAsync(transfer2, dailyLimit);
        PrintResult(result2);

        var finalSpend = await limitService.GetCurrentDailySpendAsync(userId, targetDate);
        Console.WriteLine($"\nFinal Spend for {userId} on {targetDate:yyyy-MM-dd}: ${finalSpend:N2} / ${dailyLimit:N2}");
    }

    private static void PrintResult(TransactionResult res)
    {
        var color = res.IsSuccess ? ConsoleColor.Green : ConsoleColor.Red;
        Console.ForegroundColor = color;
        Console.WriteLine(res.IsSuccess
            ? $"[SUCCESS] Tx {res.TransactionId[..8]} | Amount: ${res.Amount:N2} | TotalSpent: ${res.CurrentTotalSpent:N2}"
            : $"[REJECTED] Tx {res.TransactionId[..8]} | Amount: ${res.Amount:N2} | Reason: {res.RejectionReason} | TotalSpent: ${res.CurrentTotalSpent:N2}");
        Console.ResetColor();
    }
}
