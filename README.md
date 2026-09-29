# Daily Limit Transaction System

A .NET 9 distributed system that enforces per-user daily spending limits on scheduled transactions — even when messages arrive out of order, are delivered concurrently, or are retried by the message broker.

Built with **Redis** (atomic Lua scripts), **MassTransit**, and **Azure Service Bus Basic Tier** in a Clean Architecture layout.

---

## Problem Statement

Scheduled e-Transfers on Azure Service Bus **Basic Tier** lack session ordering and native future-message scheduling. This means:

1. **Out-of-order delivery** — a transaction scheduled for 12:00 PM can arrive at the consumer before one scheduled for 09:00 AM.
2. **Concurrent consumers** — multiple worker threads may attempt to process the same user's transactions simultaneously, risking over-allocation.
3. **Duplicate delivery** — the broker can redeliver the same message, potentially double-deducting the daily limit.

This project solves all three problems with a layered defence of **atomic Redis Lua scripts**, **distributed mutex locks**, and **idempotency guards**.

---

## Architecture

```
DailyLimitTransactionSystem/
├── src/
│   ├── Core/                  # Domain models, interfaces, contracts (zero dependencies)
│   ├── Application/           # Business logic, processing pipeline, MassTransit consumers
│   ├── Infrastructure/        # Redis, MassTransit, repository implementations
│   └── ConsoleApp/            # Entry point with 5 runnable demo scenarios
└── tests/
    └── Tests/                 # xUnit + FluentAssertions + Moq
```

### Layer Responsibilities

| Layer | Project | Purpose |
|---|---|---|
| **Core** | `DailyLimitTransactionSystem.Core` | Domain models (`Transaction`, `TransactionResult`, `DailyLimitPolicy`), enums (`TransactionStatus`, `RejectionReason`), service interfaces, and message contracts. No external dependencies. |
| **Application** | `DailyLimitTransactionSystem.Application` | `TransactionExecutionProcessor` (the core processing pipeline), `TransactionSchedulerService`, and the MassTransit `ProcessTransactionConsumer`. |
| **Infrastructure** | `DailyLimitTransactionSystem.Infrastructure` | Redis implementations (`RedisDailyLimitService`, `RedisDistributedLockService`, `RedisIdempotencyService`, `RedisMessageScheduler`), Lua scripts, MassTransit configuration, and the in-memory transaction repository. |
| **ConsoleApp** | `DailyLimitTransactionSystem.ConsoleApp` | Host process that wires up DI, connects to Redis/ASB, and runs five demonstration scenarios. |
| **Tests** | `DailyLimitTransactionSystem.Tests` | Concurrency, idempotency, reservation, and FIFO-ordering regression tests. |

---

## Concurrency Control — Defence in Depth

The `TransactionExecutionProcessor` applies three guards in sequence:

### 1. Idempotency Guard

Prevents duplicate message delivery from double-deducting spend:

```
TryAcquireExecution(txId) → already processed? → return cached result
```

- Uses Redis `SETNX` (or in-memory `ConcurrentDictionary`) with expiry.
- On first delivery: acquires an execution slot.
- On duplicate delivery: returns the previously cached `TransactionResult`.

### 2. Distributed Mutex Lock

Serialises all transactions for the same `(UserId, Date)` pair, even across multiple worker processes:

```
AcquireLock("user:{userId}:{date}") → 10s expiry, 5s wait timeout
```

- Uses Redis `SET NX EX` + atomic Lua `DEL`-if-token-matches on release.
- Falls back to a thread-safe in-memory `SemaphoreSlim` when Redis is unavailable.

### 3. Atomic Lua Check-and-Deduct

The final gate — a single Redis Lua script that atomically reads the current spend, verifies the limit, and increments in one round-trip:

```lua
local current = tonumber(redis.call('GET', key) or '0')
if (current + amount) <= (limit + tolerance) then
    local new_val = tonumber(redis.call('INCRBYFLOAT', key, amount))
    redis.call('EXPIRE', key, ttl)
    return { 1, tostring(new_val), tostring(remaining) }
else
    return { 0, tostring(current), tostring(remaining) }
end
```

- No race window — Redis executes Lua scripts atomically.
- A separate `RollbackLimit` script compensates if the downstream payment gateway fails.

---

## Scheduling

### Execute-Time Enforcement

Transactions are scheduled freely via `TransactionSchedulerService` (save + publish to the message bus). The daily limit is checked and enforced **at execution time** inside the `TransactionExecutionProcessor` pipeline — this keeps scheduling simple and lets the processor handle all concurrency concerns.

### Basic Tier Scheduling — Redis Sorted Set Poller

Since Azure Service Bus Basic Tier doesn't support `ScheduledEnqueueTimeUtc`, a `RedisMessageScheduler` stores transactions in a Redis Sorted Set keyed by scheduled time:

```
ZADD schedule:{date} {timestamp} {serialized-transaction}
```

A poller retrieves due transactions with `ZRANGEBYSCORE` and dispatches them in **creation-order FIFO** (Bug #2 fix — see below).

---

## Key Bug Fixes

### Bug #1 — Daily Limit Exceeded on Scheduled e-Transfers

**Symptom:** Two scheduled transfers of $2,200 and $1,500 both succeeded, sending $3,700 against a $3,000 limit.

**Root Cause:** Non-atomic read-then-write on the daily spend counter.

**Fix:** Replaced with a single atomic Redis Lua script that checks and deducts in one operation.

### Bug #2 — Scheduled e-Transfers Not Processed in Creation Order

**Symptom:** When multiple transfers share the same scheduled execution time, the earliest-created transfer was rejected instead of the latest.

**Root Cause:** The scheduler dispatched transactions in insertion order rather than creation order.

**Fix:** `RedisMessageScheduler.GetAndPollDueTransactionsAsync` now sorts by `CreatedAt` before dispatch, guaranteeing creation-order FIFO.

---

## Demo Scenarios

The console app runs four scenarios end-to-end:

| # | Scenario | What It Demonstrates |
|---|---|---|
| 1 | **Irregular Arrival Order** | T2→T1→T3 out-of-order arrival; daily limit still enforced correctly |
| 2 | **High Concurrency Race** | 10 parallel threads attempt $500 each ($5,000 total); exactly 6 succeed ($3,000) |
| 3 | **Redis Sorted Set Polling** | Basic Tier workaround: transactions stored with `ZADD`, polled with `ZRANGEBYSCORE`, dispatched in order |
| 4 | **Bug #2 FIFO Regression** | Scrambled insertion order; creation-order FIFO dispatch verified |

---

## Prerequisites

- [.NET 9 SDK](https://dotnet.microsoft.com/download/dotnet/9.0)
- **Redis** (optional) — the system falls back to a thread-safe in-memory emulation engine when Redis is unreachable
- **Azure Service Bus** (optional) — falls back to MassTransit's in-memory transport

## Getting Started

### 1. Clone & Restore

```bash
git clone <repository-url>
cd DailyLimitTransactionSystem
dotnet restore
```

### 2. Configure (Optional)

Edit `src/DailyLimitTransactionSystem.ConsoleApp/appsettings.json`:

```json
{
  "Redis": {
    "ConnectionString": "localhost:6379,abortConnect=false,connectTimeout=1000"
  },
  "AzureServiceBus": {
    "ConnectionString": "",
    "QueueName": "transaction-processing-queue",
    "ConcurrentMessageLimit": 10
  },
  "TransactionLimits": {
    "DefaultDailyLimit": 3000.00,
    "Currency": "CAD"
  }
}
```

> Leave `AzureServiceBus:ConnectionString` empty to use the in-memory transport.  
> Leave Redis unreachable to use the in-memory atomic emulation engine.

### 3. Run the Demo

```bash
dotnet run --project src/DailyLimitTransactionSystem.ConsoleApp
```

### 4. Run the Tests

```bash
dotnet test
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Runtime | .NET 9 / C# 13 |
| Message Broker | MassTransit 8.3 + Azure Service Bus Basic Tier (or in-memory) |
| Distributed State | StackExchange.Redis 3.3 (atomic Lua scripts) |
| Testing | xUnit 2.9, FluentAssertions 8.11, Moq 4.20, Coverlet |
| DI / Hosting | Microsoft.Extensions.DependencyInjection / Hosting |

---

## Project Structure — Key Files

```
src/
├── Core/
│   ├── Models/
│   │   ├── Transaction.cs              # Transaction entity
│   │   ├── TransactionResult.cs        # Success/Rejected result with factory methods
│   │   └── Enums.cs                    # TransactionStatus, RejectionReason
│   ├── Interfaces/
│   │   └── Interfaces.cs              # IDailyLimitService, IDistributedLockService,
│   │                                  # IIdempotencyService, ITransactionRepository,
│   │                                  # IMessagePublisher, IMessageConsumer
│   └── Contracts/
│       └── Messages.cs                # ExecuteTransactionCommand, Completed/Rejected events
├── Application/
│   ├── Services/
│   │   ├── TransactionExecutionProcessor.cs   # Core pipeline (idempotency → lock → Lua → bank)
│   │   └── TransactionSchedulerService.cs     # Schedule + publish to bus, enforce at execution
│   └── Consumers/
│       └── ProcessTransactionConsumer.cs       # MassTransit IConsumer<ExecuteTransactionCommand>
├── Infrastructure/
│   ├── Redis/
│   │   ├── LuaScripts.cs                      # CheckAndDeductLimit, RollbackLimit, ReleaseLock
│   │   ├── RedisDailyLimitService.cs           # IDailyLimitService (Redis + in-memory fallback)
│   │   ├── RedisDistributedLockService.cs      # IDistributedLockService
│   │   ├── RedisIdempotencyService.cs          # IIdempotencyService
│   │   └── RedisMessageScheduler.cs            # ZADD/ZRANGEBYSCORE scheduler for Basic Tier
│   ├── MassTransit/
│   │   ├── MassTransitConfiguration.cs         # ASB / in-memory bus setup
│   │   └── MassTransitMessagePublisher.cs      # IMessagePublisher via IPublishEndpoint
│   └── Repositories/
│       └── InMemoryTransactionRepository.cs    # ITransactionRepository (ConcurrentDictionary)
└── ConsoleApp/
    ├── Program.cs                              # DI wiring + 5 demo scenarios
    ├── appsettings.json                        # Redis, ASB, limit configuration
    └── appsettings.Development.json
tests/
└── Tests/
    └── ConcurrencyAndLimitTests.cs             # 6 test cases covering all scenarios
```

---

## License

This project is provided as-is for demonstration and reference purposes.
