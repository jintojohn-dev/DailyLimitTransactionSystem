namespace DailyLimitTransactionSystem.Infrastructure.Redis;

public static class LuaScripts
{
    /// <summary>
    /// Atomic Lua script to check if the current spend + requested amount is within daily limit.
    /// If within limit, increments the spend and sets TTL. Returns [1, new_spend, remaining_limit].
    /// If exceeded, makes no changes and returns [0, current_spend, remaining_limit].
    /// </summary>
    public const string CheckAndDeductLimit = @"
        local key = KEYS[1]
        local amount = tonumber(ARGV[1])
        local limit = tonumber(ARGV[2])
        local ttl = tonumber(ARGV[3])

        local current = tonumber(redis.call('GET', key) or '0')
        local tolerance = 0.000001

        if (current + amount) <= (limit + tolerance) then
            local new_val = tonumber(redis.call('INCRBYFLOAT', key, amount))
            local current_ttl = redis.call('TTL', key)
            if current_ttl == -1 or current_ttl == -2 or current == 0 then
                redis.call('EXPIRE', key, ttl)
            end
            local remaining = math.max(0, limit - new_val)
            return { 1, tostring(new_val), tostring(remaining) }
        else
            local remaining = math.max(0, limit - current)
            return { 0, tostring(current), tostring(remaining) }
        end
    ";





    /// <summary>
    /// Atomic Lua script to rollback spend if a downstream operation fails.
    /// </summary>
    public const string RollbackLimit = @"
        local key = KEYS[1]
        local amount = tonumber(ARGV[1])

        local current = tonumber(redis.call('GET', key) or '0')
        local new_val = math.max(0, current - amount)
        redis.call('SET', key, tostring(new_val), 'KEEPTTL')
        return tostring(new_val)
    ";

    /// <summary>
    /// Atomic script to release a distributed lock only if the token matches.
    /// </summary>
    public const string ReleaseLock = @"
        if redis.call('GET', KEYS[1]) == ARGV[1] then
            return redis.call('DEL', KEYS[1])
        else
            return 0
        end
    ";

    /// <summary>
    /// Atomically fetches all members with score <= maxScore from a sorted set and removes them.
    /// Returns an array of members that were removed.
    /// Args: KEYS[1] = sorted set key, ARGV[1] = maxScore
    /// </summary>
    public const string FetchAndRemoveDue = @"
        local key = KEYS[1]
        local maxScore = tonumber(ARGV[1])
        local members = redis.call('ZRANGEBYSCORE', key, 0, maxScore)
        if #members == 0 then
            return members
        end
        for i=1,#members do
            redis.call('ZREM', key, members[i])
        end
        return members
    ";
}
