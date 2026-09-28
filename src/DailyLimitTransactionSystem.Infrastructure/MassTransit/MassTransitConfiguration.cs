using DailyLimitTransactionSystem.Core.Contracts;
using DailyLimitTransactionSystem.Core.Interfaces;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;

namespace DailyLimitTransactionSystem.Infrastructure.MassTransit;

public static class MassTransitConfiguration
{
    /// <summary>
    /// Configures MassTransit for Azure Service Bus Basic Tier (Queues only, no Topics, no Sessions)
    /// with Redis handling distributed locking and atomic limit enforcement.
    /// All settings are driven by appsettings.json configuration.
    /// </summary>
    public static IServiceCollection AddDailyLimitMassTransit(
        this IServiceCollection services,
        Action<IBusRegistrationConfigurator>? configureConsumers = null,
        string? azureServiceBusConnectionString = null,
        string queueName = "transaction-processing-queue",
        int concurrentMessageLimit = 10)
    {
        services.AddMassTransit(x =>
        {
            configureConsumers?.Invoke(x);

            if (!string.IsNullOrWhiteSpace(azureServiceBusConnectionString))
            {
                x.UsingAzureServiceBus((context, cfg) =>
                {
                    cfg.Host(azureServiceBusConnectionString);

                    // Azure Service Bus Basic Tier: Point-to-point Queue
                    // (Basic Tier does not support Topics/Subscriptions or Sessions)
                    cfg.ReceiveEndpoint(queueName, e =>
                    {
                        e.ConcurrentMessageLimit = concurrentMessageLimit;
                    });

                    cfg.ConfigureEndpoints(context);
                });
            }
            else
            {
                x.UsingInMemory((context, cfg) =>
                {
                    cfg.ReceiveEndpoint(queueName, e =>
                    {
                        e.ConcurrentMessageLimit = concurrentMessageLimit;
                    });

                    cfg.ConfigureEndpoints(context);
                });
            }
        });

        services.AddScoped<IMessagePublisher, MassTransitMessagePublisher>();

        return services;
    }
}

