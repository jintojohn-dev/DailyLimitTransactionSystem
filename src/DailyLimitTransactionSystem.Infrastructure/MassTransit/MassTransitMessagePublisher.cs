using DailyLimitTransactionSystem.Core.Contracts;
using DailyLimitTransactionSystem.Core.Interfaces;
using DailyLimitTransactionSystem.Core.Models;
using MassTransit;
using Microsoft.Extensions.Logging;

namespace DailyLimitTransactionSystem.Infrastructure.MassTransit;

public class MassTransitMessagePublisher : IMessagePublisher
{
    private readonly IPublishEndpoint _publishEndpoint;
    private readonly ILogger<MassTransitMessagePublisher> _logger;

    public MassTransitMessagePublisher(
        IPublishEndpoint publishEndpoint,
        ILogger<MassTransitMessagePublisher> logger)
    {
        _publishEndpoint = publishEndpoint;
        _logger = logger;
    }

    public async Task PublishTransactionScheduledAsync(Transaction transaction, CancellationToken ct = default)
    {
        var command = new ExecuteTransactionCommand
        {
            TransactionId = transaction.Id,
            UserId = transaction.UserId,
            Amount = transaction.Amount,
            ScheduledExecutionTime = transaction.ScheduledExecutionTime,
            TargetDate = transaction.TargetDate,
            Description = transaction.Description
        };

        // Azure Service Bus Basic Tier does NOT support:
        //   - Sessions (SessionId) — requires Standard/Premium tier
        //   - Partition keys — requires Standard/Premium tier
        //   - ScheduledEnqueueTimeUtc — requires Standard/Premium tier
        // Instead, ordering and concurrency are handled by Redis distributed locks + atomic Lua scripts.
        // Future scheduling is handled by RedisMessageScheduler (ZADD sorted set poller).
        await _publishEndpoint.Publish(command, context =>
        {
            context.MessageId = Guid.TryParse(transaction.Id, out var msgId) ? msgId : NewId.NextGuid();
        }, ct);

        _logger.LogInformation(
            "[MassTransit Publisher] Published ExecuteTransactionCommand for Tx {TxId} | User: {UserId} | Amount: ${Amount:N2} | ScheduledFor: {ScheduledTime:yyyy-MM-dd HH:mm:ss}",
            transaction.Id[..8], transaction.UserId, transaction.Amount, transaction.ScheduledExecutionTime
        );
    }

    public async Task PublishTransactionCompletedAsync(TransactionResult result, CancellationToken ct = default)
    {
        if (result.IsSuccess)
        {
            await _publishEndpoint.Publish(new TransactionCompletedEvent
            {
                TransactionId = result.TransactionId,
                UserId = result.UserId,
                Amount = result.Amount,
                CurrentTotalSpent = result.CurrentTotalSpent,
                DailyLimit = result.DailyLimit,
                RemainingLimit = result.RemainingLimit,
                IsSuccess = true
            }, ct);
        }
        else
        {
            await _publishEndpoint.Publish(new TransactionRejectedEvent
            {
                TransactionId = result.TransactionId,
                UserId = result.UserId,
                Amount = result.Amount,
                CurrentTotalSpent = result.CurrentTotalSpent,
                DailyLimit = result.DailyLimit,
                RemainingLimit = result.RemainingLimit,
                Reason = result.RejectionReason,
                Message = result.Message
            }, ct);
        }

        _logger.LogInformation(
            "[MassTransit Publisher] Published Result Event for Tx {TxId}: Success={Success}, Total Spend: ${Spend:N2}",
            result.TransactionId[..8], result.IsSuccess, result.CurrentTotalSpent
        );
    }
}
