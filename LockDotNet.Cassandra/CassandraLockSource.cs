namespace LockDotNet.Cassandra
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Cassandra;
    using LockDotNet;

    public class CassandraLockSource : ILockSource
    {
        private const string LocksTable = "distributed_locks";

        private static readonly SemaphoreSlim initializationSemaphore = new SemaphoreSlim(1);

        private static bool isInitialized = false;

        private readonly ISession session;

        private PreparedStatement upsertStatement;
        private PreparedStatement deleteStatement;

        public CassandraLockSource(ISession session)
        {
            this.session = session;
        }

        public async Task<Lock> AcquireAsync(
            string lockKey, TimeSpan lockTtl, CancellationToken cancellationToken = default)
        {
            await this.InitializeAsync();

            var lockId = Guid.NewGuid();

            var preparedStatement = await this.GetUpsertPreparedStatementAsync();
            var boundStatement = preparedStatement.Bind(lockKey, lockId, (int)lockTtl.TotalSeconds);

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var rowSet = await session.ExecuteAsync(boundStatement);

                if (IsApplied(rowSet))
                {
                    break;
                }

                await Task.Delay(10, cancellationToken);
            }
            while(true);

            return new Lock(lockKey, lockId, @lock => this.ReleaseAsync(@lock));
        }

        private static bool IsApplied(RowSet rowSet)
        {
            return rowSet?.FirstOrDefault()?.GetValue<bool>("[applied]") ?? false;
        }

        private async Task InitializeAsync()
        {
            if (isInitialized)
            {
                return;
            }

            await initializationSemaphore.WaitAsync();

            try
            {
                if (isInitialized)
                {
                    return;
                }

                await this.session.ExecuteAsync(new SimpleStatement(
                    $"CREATE TABLE IF NOT EXISTS {LocksTable} (" +
                        "lock_key text," +
                        "lock_id uuid," +
                        "PRIMARY KEY ((lock_key))" +
                    ");"));

                isInitialized = true;
            }
            finally
            {
                initializationSemaphore.Release();
            }
        }

        private async Task ReleaseAsync(Lock @lock)
        {
            var preparedStatement = await this.GetDeletePreparedStatementAsync();

            await session.ExecuteAsync(preparedStatement.Bind(@lock.Key, @lock.Id));
        }

        private async ValueTask<PreparedStatement> GetUpsertPreparedStatementAsync()
        {
            if (this.upsertStatement != null)
            {
                return this.upsertStatement;
            }

            this.upsertStatement = await this.session.PrepareAsync(
                $"INSERT INTO {LocksTable} (lock_key, lock_id) VALUES (?, ?) IF NOT EXISTS USING TTL ?;");

            return this.upsertStatement;
        }

        private async ValueTask<PreparedStatement> GetDeletePreparedStatementAsync()
        {
            if (this.deleteStatement != null)
            {
                return this.deleteStatement;
            }

            this.deleteStatement = await this.session.PrepareAsync(
                $"DELETE FROM {LocksTable} WHERE lock_key = ? IF lock_id = ?;");

            return this.deleteStatement;
        }
    }
}
