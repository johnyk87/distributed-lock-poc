namespace LockDotNet.MongoDb
{
    using System;
    using System.Linq;
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
                LockId = lockId,
                LockKey = lockKey,
            };

            do
            {
                cancellationToken.ThrowIfCancellationRequested();

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
                
                await this.collection.Indexes.CreateOneAsync(
                    new CreateIndexModel<LockEntity>(
                        Builders<LockEntity>.IndexKeys.Ascending(l => l.LockKey),
                        new CreateIndexOptions<LockEntity>
                        {
                            Name = "unique_lock_key",
                            Background = false,
                            Unique = true,
                        }));

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
                await this.collection.InsertOneAsync(@lock);

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
