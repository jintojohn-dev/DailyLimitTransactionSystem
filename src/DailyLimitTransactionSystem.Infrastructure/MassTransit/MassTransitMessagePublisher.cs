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

        await _publishEndpoint.Publish(command, context =>
        {
            try
            {
                // In MassTransit with Azure Service Bus, we set SessionId and PartitionKey to the UserId
                // This ensures Azure Service Bus enforces strict FIFO ordering per user session!
                context.SetSessionId(transaction.UserId);
                context.SetPartitionKey(transaction.UserId);

                if (transaction.ScheduledExecutionTime > DateTime.UtcNow)
                {
                    context.SetScheduledEnqueueTime(transaction.ScheduledExecutionTime);
                }
            }
            catch
            {
                // Non-ASB or In-Memory test fallback doesn't support session contexts
            }

            context.MessageId = Guid.TryParse(transaction.Id, out var msgId) ? msgId : NewId.NextGuid();
        }, ct);

        _logger.LogInformation(
            "[MassTransit Publisher] Published ExecuteTransactionCommand for Tx {TxId} | User/Session: {UserId} | Amount: ${Amount:N2} | Enqueue: {Enqueue:yyyy-MM-dd HH:mm:ss}",
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
            }, context =>
            {
                try
                {
                    context.SetSessionId(result.UserId);
                    context.SetPartitionKey(result.UserId);
                }
                catch
                {
                    // Fallback
                }
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
            }, context =>
            {
                try
                {
                    context.SetSessionId(result.UserId);
                    context.SetPartitionKey(result.UserId);
                }
                catch
                {
                    // Fallback
                }
            }, ct);
        }

        _logger.LogInformation(
            "[MassTransit Publisher] Published Result Event for Tx {TxId}: Success={Success}, Total Spend: ${Spend:N2}",
            result.TransactionId[..8], result.IsSuccess, result.CurrentTotalSpent
        );
    }
}
