namespace LockDotNet.MongoDb
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using LockDotNet;
    using MongoDB.Driver;

    public class MongoDbLockSource : ILockSource
    {
        private const string LocksCollection = "locks";

        private readonly SemaphoreSlim initializationSemaphore = new SemaphoreSlim(1);

        private bool isInitialized = false;

        private readonly IMongoDatabase database;
        private readonly IMongoCollection<LockEntity> collection;

        public MongoDbLockSource(IMongoDatabase database)
        {
            this.database = database ?? throw new ArgumentNullException(nameof(database));

            this.collection = database.GetCollection<LockEntity>(LocksCollection);
        }

        public async Task<Lock> AcquireAsync(
            string lockKey, TimeSpan lockTtl, CancellationToken cancellationToken = default)
        {
            await this.InitializeAsync();

            var lockId = Guid.NewGuid();
            var @lock = new LockEntity
            {
                LockKey = lockKey,
                LockId = lockId,
            };

            do
            {
                @lock.ExpirationDate = DateTime.UtcNow.Add(lockTtl);

                if (await this.TryInsertLock(@lock))
                {
                    break;
                }

                await Task.Delay(10, cancellationToken);
            }
            while(true);

            return new Lock(lockKey, lockId, this.ReleaseAsync);
        }

        private async Task InitializeAsync()
        {
            if (this.isInitialized)
            {
                return;
            }

            await this.initializationSemaphore.WaitAsync();

            try
            {
                if (this.isInitialized)
                {
                    return;
                }
                
                await this.collection.Indexes.CreateManyAsync(
                    new[]
                    {
                        new CreateIndexModel<LockEntity>(
                            Builders<LockEntity>.IndexKeys.Ascending(l => l.LockKey),
                            new CreateIndexOptions<LockEntity>
                            {
                                Name = "unique_key",
                                Background = false,
                                Unique = true,
                            }),
                        new CreateIndexModel<LockEntity>(
                            Builders<LockEntity>.IndexKeys.Ascending(l => l.ExpirationDate),
                            new CreateIndexOptions<LockEntity>
                            {
                                Name = "expiration_ttl",
                                Background = false,
                                ExpireAfter = TimeSpan.Zero,
                            }),
                    });

                this.isInitialized = true;
            }
            finally
            {
                this.initializationSemaphore.Release();
            }
        }

        private async Task<bool> TryInsertLock(LockEntity @lock)
        {
            try
            {
                await this.collection.UpdateOneAsync<LockEntity>(
                    l => l.LockKey == @lock.LockKey && l.ExpirationDate < DateTime.UtcNow,
                    Builders<LockEntity>.Update
                        .SetOnInsert(l => l.LockKey, @lock.LockKey)
                        .Set(l => l.LockId, @lock.LockId)
                        .Set(l => l.ExpirationDate, @lock.ExpirationDate),
                    new UpdateOptions { IsUpsert = true });

                return true;
            }
            catch (MongoWriteException ex) when (ex.WriteError?.Category == ServerErrorCategory.DuplicateKey)
            {
                return false;
            }
        }

        private Task ReleaseAsync(Lock @lock)
        {
            return this.collection.DeleteOneAsync(l => l.LockKey == @lock.Key && l.LockId == @lock.Id);
        }
    }
}
