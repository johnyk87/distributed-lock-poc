namespace DistributedLockPoc
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Cassandra;

    public class CassandraLockManager : IDistributedLockManager
    {
        private const string LocksTable = "distributed_locks";

        private static readonly SemaphoreSlim initializationSemaphore = new SemaphoreSlim(1);

        private static bool isInitialized = false;

        private readonly ISession session;

        private PreparedStatement upsertStatement;
        private PreparedStatement deleteStatement;

        public CassandraLockManager(ISession session)
        {
            this.session = session;
        }

        public async Task<DistributedLock> AcquireAsync(
            string lockKey, TimeSpan lockTtl, TimeSpan retryInterval, CancellationToken cancellationToken = default)
        {
            await this.InitializeAsync();

            var lockId = Guid.NewGuid();

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var preparedStatement = await this.GetUpsertPreparedStatementAsync();

                var rowSet = await session.ExecuteAsync(
                    preparedStatement.Bind(lockKey, lockId, (int)lockTtl.TotalSeconds));

                if (IsApplied(rowSet))
                {
                    break;
                }

                await Task.Delay(retryInterval, cancellationToken);
            }
            while(true);

            return new DistributedLock(this, lockKey, lockId);
        }

        public async Task<bool> ReleaseAsync(DistributedLock distributedLock)
        {
            await this.InitializeAsync();

            var preparedStatement = await this.GetDeletePreparedStatementAsync();

            var rowSet = await session.ExecuteAsync(
                preparedStatement.Bind(distributedLock.Key, distributedLock.Id));
            
            return IsApplied(rowSet);
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

        private async ValueTask<PreparedStatement> GetUpsertPreparedStatementAsync()
        {
            if (upsertStatement != null)
            {
                return upsertStatement;
            }

            upsertStatement = await this.session.PrepareAsync(
                $"INSERT INTO {LocksTable} (lock_key, lock_id) VALUES (?, ?) IF NOT EXISTS USING TTL ?;");

            return upsertStatement;
        }

        private async ValueTask<PreparedStatement> GetDeletePreparedStatementAsync()
        {
            if (deleteStatement != null)
            {
                return deleteStatement;
            }

            deleteStatement = await this.session.PrepareAsync(
                $"DELETE FROM {LocksTable} WHERE lock_key = ? IF lock_id = ?;");

            return deleteStatement;
        }
    }
}
