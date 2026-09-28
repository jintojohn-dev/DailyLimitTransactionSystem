using DailyLimitTransactionSystem.Application.Services;
using DailyLimitTransactionSystem.Core.Contracts;
using DailyLimitTransactionSystem.Core.Models;
using MassTransit;
using Microsoft.Extensions.Logging;

namespace DailyLimitTransactionSystem.Application.Consumers;

public class ProcessTransactionConsumer : IConsumer<ExecuteTransactionCommand>
{
    private readonly TransactionExecutionProcessor _processor;
    private readonly ILogger<ProcessTransactionConsumer> _logger;

    public ProcessTransactionConsumer(
        TransactionExecutionProcessor processor,
        ILogger<ProcessTransactionConsumer> logger)
    {
        _processor = processor;
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<ExecuteTransactionCommand> context)
    {
        var command = context.Message;
        _logger.LogInformation(
            "[MassTransit Consumer] Processing ExecuteTransactionCommand for Tx {TxId} | User: {UserId} | Amount: ${Amount:N2} | Scheduled: {Time:HH:mm:ss}",
            command.TransactionId[..8], command.UserId, command.Amount, command.ScheduledExecutionTime
        );

        var transaction = new Transaction
        {
            Id = command.TransactionId,
            UserId = command.UserId,
            Amount = command.Amount,
            ScheduledExecutionTime = command.ScheduledExecutionTime,
            TargetDate = command.TargetDate,
            Description = command.Description,
            Status = TransactionStatus.Scheduled
        };

        var result = await _processor.ProcessTransactionAsync(transaction, ct: context.CancellationToken);

        if (result.IsSuccess)
        {
            await context.Publish(new TransactionCompletedEvent
            {
                TransactionId = result.TransactionId,
                UserId = result.UserId,
                Amount = result.Amount,
                CurrentTotalSpent = result.CurrentTotalSpent,
                DailyLimit = result.DailyLimit,
                RemainingLimit = result.RemainingLimit,
                IsSuccess = true
            }, context.CancellationToken);
        }
        else
        {
            await context.Publish(new TransactionRejectedEvent
            {
                TransactionId = result.TransactionId,
                UserId = result.UserId,
                Amount = result.Amount,
                CurrentTotalSpent = result.CurrentTotalSpent,
                DailyLimit = result.DailyLimit,
                RemainingLimit = result.RemainingLimit,
                Reason = result.RejectionReason,
                Message = result.Message
            }, context.CancellationToken);
        }
    }
}
