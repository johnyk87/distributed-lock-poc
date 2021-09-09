namespace DistributedLockPoc
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Cassandra;
    using LockDotNet;
    using LockDotNet.Cassandra;

    public static class Program
    {
        private const string LocksTable = "locks";
        private const string MyLockKey = "my_lock";
        private const string AnotherLockKey = "another_lock";

        private static readonly TimeSpan LockTtl = TimeSpan.FromSeconds(10);

        public static async Task Main()
        {
            var session = default(ISession);

            try
            {
                var cluster = Cluster.Builder().AddContactPoints("127.0.0.1").Build();
                session = await cluster.ConnectAsync();

                InitializeKeyspace(session);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.GetType().FullName}: {ex.Message}");
                Console.WriteLine($"StackTrace:{Environment.NewLine}{ex.StackTrace}");

                Environment.Exit(1);
                return;
            }

            try
            {
                var lockSource = (ILockSource)new CassandraLockSource(session);

                Console.WriteLine($"Acquiring {MyLockKey}");
                var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
                await using (var myLock =
                    await lockSource.AcquireAsync(MyLockKey, LockTtl, cts.Token))
                {
                    Console.WriteLine($"{myLock.Key} acquired: {myLock.Id}");

                    await PrintLocksAsync(session);

                    Console.WriteLine($"Acquiring {AnotherLockKey}");
                    cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
                    await using (var anotherLock =
                        await lockSource.AcquireAsync(AnotherLockKey, LockTtl, cts.Token))
                    {
                        Console.WriteLine($"{anotherLock.Key} acquired: {anotherLock.Id}");

                        await PrintLocksAsync(session);

                        Console.WriteLine($"Disposing {anotherLock.Key}");
                    }

                    await PrintLocksAsync(session);

                    Console.WriteLine($"Acquiring {AnotherLockKey} again");
                    cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
                    await using (var anotherLock =
                        await lockSource.AcquireAsync(AnotherLockKey, LockTtl, cts.Token))
                    {
                        Console.WriteLine($"{anotherLock.Key} acquired: {anotherLock.Id}");

                        await PrintLocksAsync(session);

                        Console.WriteLine($"Disposing {anotherLock.Key}");
                    }

                    await PrintLocksAsync(session);

                    Console.WriteLine($"Acquiring {MyLockKey} again");
                    cts = new CancellationTokenSource(LockTtl + TimeSpan.FromSeconds(1));
                    await using (var secondMyLock =
                        await lockSource.AcquireAsync(MyLockKey, LockTtl, cts.Token))
                    {
                        Console.WriteLine($"{secondMyLock.Key} acquired: {secondMyLock.Id}");

                        await PrintLocksAsync(session);

                        Console.WriteLine($"Disposing {secondMyLock.Key}");
                    }

                    await PrintLocksAsync(session);

                    Console.WriteLine($"Disposing {myLock.Key}");
                }

                await PrintLocksAsync(session);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{ex.GetType().FullName}: {ex.Message}");
                Console.WriteLine($"StackTrace:{Environment.NewLine}{ex.StackTrace}");

                try
                {
                    await PrintLocksAsync(session);
                }
                catch { }

                Environment.Exit(1);
                return;
            }
        }

        private static void InitializeKeyspace(ISession session)
        {
            const string keyspaceName = "poc_distributed_locks";
            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
        }

        private static async Task PrintLocksAsync(ISession session)
        {
            Console.WriteLine();

            var rowSet = await session.ExecuteAsync(new SimpleStatement($"SELECT * FROM {LocksTable}"));

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
    }
}
