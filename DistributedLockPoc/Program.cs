namespace DistributedLockPoc
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Cassandra;

    public static class Program
    {
        private const string DistributedLocksTable = "distributed_locks";

        public static async Task Main()
        {
            try
            {
                var cluster = Cluster.Builder().AddContactPoints("127.0.0.1").Build();
                var session = await cluster.ConnectAsync();

                InitializeKeyspace(session);
                await InitializeDistributedLocksAsync(session);

                await PrintLocksAsync(session);

                Console.WriteLine("Acquiring first lock");
                var lockId = await AcquireLockAsync(session, "my_lock", TimeSpan.FromSeconds(10));
                Console.WriteLine($"First lock acquired: {lockId}");

                await PrintLocksAsync(session);

                Console.WriteLine("Acquiring second lock");
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                var lockId2 = await AcquireLockAsync(session, "my_lock", TimeSpan.FromSeconds(10), cts.Token);
                Console.WriteLine($"Second lock acquired: {lockId2}");

                await PrintLocksAsync(session);

                Console.WriteLine("Releasing first lock");
                var released = await ReleaseLockAsync(session, "my_lock", lockId);
                Console.WriteLine($"First lock release {(released ? "succeeded" : "failed")}");

                await PrintLocksAsync(session);

                Console.WriteLine("Releasing second lock");
                released = await ReleaseLockAsync(session, "my_lock", lockId2);
                Console.WriteLine($"Second lock release {(released ? "succeeded" : "failed")}");

                await PrintLocksAsync(session);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.GetType().FullName}: {ex.Message}");
                Console.WriteLine($"StackTrace:{Environment.NewLine}{ex.StackTrace}");
            }
        }

        private static void InitializeKeyspace(ISession session)
        {
            const string keyspaceName = "poc_distributed_locks";
            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
        }

        private static Task InitializeDistributedLocksAsync(ISession session)
        {
            return session.ExecuteAsync(new SimpleStatement(
                $"create table if not exists {DistributedLocksTable} (" +
                    "lock_key text," +
                    "lock_id uuid," +
                    "primary key ((lock_key))" +
                ");"));
        }

        private static async Task PrintLocksAsync(ISession session)
        {
            Console.WriteLine();

            var rowSet = await session.ExecuteAsync(new SimpleStatement($"SELECT * FROM {DistributedLocksTable}"));

            var isFirst = true;
            foreach (var column in rowSet.Columns)
            {
                Console.Write($"{(isFirst ? "" : "\t")}{column.Name}");
                isFirst = false;
            }

            Console.WriteLine();

            foreach (var row in rowSet)
            {
                isFirst = true;
                foreach (var column in rowSet.Columns)
                {
                    var value = row.GetValue(column.Type, column.Name);

                    Console.Write($"{(isFirst ? "" : "\t")}{value?.ToString() ?? "<null>"}");

                    isFirst = false;
                }

                Console.WriteLine();
            }

            Console.WriteLine();
        }

        private static async Task<Guid> AcquireLockAsync(
            ISession session, string lockKey, TimeSpan lockTtl, CancellationToken cancellationToken = default)
        {
            var lockId = Guid.NewGuid();

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                var rowSet = await session.ExecuteAsync(new SimpleStatement(
                    $"insert into {DistributedLocksTable} (lock_key, lock_id) values (?, ?) if not exists using ttl ?;",
                    lockKey,
                    lockId,
                    (int)lockTtl.TotalSeconds));

                if (IsApplied(rowSet))
                {
                    break;
                }

                await Task.Delay(50, cancellationToken);
            }
            while(true);

            return lockId;
        }

        private static async Task<bool> ReleaseLockAsync(ISession session, string lockKey, Guid lockId)
        {
            var rowSet = await session.ExecuteAsync(new SimpleStatement(
                $"delete from {DistributedLocksTable} where lock_key = ? if lock_id = ?;",
                lockKey,
                lockId));
            
            return IsApplied(rowSet);
        }

        private static bool IsApplied(RowSet rowSet)
        {
            return rowSet?.FirstOrDefault()?.GetValue<bool>("[applied]") ?? false;
        }
    }
}
