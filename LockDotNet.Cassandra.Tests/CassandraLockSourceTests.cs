namespace LockDotNet.Cassandra.Tests
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Cassandra;
    using LockDotNet.Cassandra;
    using Xunit;

    public class CassandraLockSourceTests
    {
        private const string TestKeyspace = "locks_tests";
        private const string LocksTable = "locks";

        private static readonly TimeSpan DefaultTtl = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan ShortTtl = TimeSpan.FromSeconds(1);

        private string RandomKey => Guid.NewGuid().ToString();

        private readonly ICluster cluster;
        private readonly ISession session;
        private readonly ILockSource lockSource;

        public CassandraLockSourceTests()
        {
            this.cluster = Cluster.Builder()
                .AddContactPoint("127.0.0.1")
                .WithDefaultKeyspace(TestKeyspace)
                .Build();

            this.session = ((Cluster)this.cluster).ConnectAndCreateDefaultKeyspaceIfNotExists();

            this.lockSource = new CassandraLockSource(this.session);
        }

        [Fact]
        public void Constructor_WithNullSession_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new CassandraLockSource(null));
        }

        [Fact]
        public async Task AcquireAsync_OnFirstCallAndWithoutTable_CreatesTable()
        {
            // Arrange
            await this.DeleteTableIfExistsAsync(LocksTable);

            var newLockSource = new CassandraLockSource(this.session);

            // Act
            await newLockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Assert
            await AssertTableExistsAsync(LocksTable);
        }

        [Fact]
        public async Task AcquireAsync_WithNoLockForTheSameKey_AcquiresLock()
        {
            // Arrange
            var lockKey = RandomKey;

            // Act
            await using var @lock = await this.lockSource.AcquireAsync(lockKey, DefaultTtl);

            // Assert
            Assert.NotNull(@lock);
            Assert.Equal(lockKey, @lock.Key);
            Assert.NotEqual(Guid.Empty, @lock.Id);

            Assert.True(await this.LockExistsAsync(@lock));
        }

        [Fact]
        public async Task AcquireAsync_WithExistingLockForTheSameKey_AcquiresLockAfterExistingLockIsReleased()
        {
            // Arrange
            var @firstLock = await this.lockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Act
            var secondLockTask = this.lockSource.AcquireAsync(@firstLock.Key, DefaultTtl);
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.False(secondLockTask.IsCompleted);

            await @firstLock.DisposeAsync();
            var @secondLock = await secondLockTask;

            // Assert
            Assert.True(await this.LockExistsAsync(@secondLock));
        }

        [Fact]
        public async Task AcquireAsync_AfterTtlExpired_LockNoLongerExists()
        {
            // Act
            var @lock = await this.lockSource.AcquireAsync(RandomKey, ShortTtl);
            await Task.Delay(ShortTtl + TimeSpan.FromMilliseconds(100));

            // Assert
            Assert.False(await this.LockExistsAsync(@lock));
        }

        [Fact]
        public async Task AcquireAsync_WithTwoLocksWithDistinctKeys_BothLocksAreAcquired()
        {
            // Act
            await using var @firstLock = await this.lockSource.AcquireAsync(RandomKey, DefaultTtl);
            await using var @secondLock = await this.lockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Assert
            Assert.True(await this.LockExistsAsync(@firstLock));
            Assert.True(await this.LockExistsAsync(@secondLock));
        }

        [Fact]
        public async Task AcquireAsync_WithCanceledToken_ThrowsOperationCanceledExceptionAndDoesNotAcquireLock()
        {
            // Arrange
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            var key = RandomKey;

            // Act
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => this.lockSource.AcquireAsync(key, DefaultTtl, cts.Token));

            // Assert
            Assert.False(await this.LockExistsAsync(key));
        }

        [Fact]
        public async Task AcquireAsync_CancellingWhileWaitingForExistingToken_ThrowsOperationCanceledExceptionAndExistingLockSurvives()
        {
            // Arrange
            await using var @firstLock = await this.lockSource.AcquireAsync(RandomKey, DefaultTtl);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

            // Act
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => this.lockSource.AcquireAsync(@firstLock.Key, DefaultTtl, cts.Token));

            // Assert
            Assert.True(await this.LockExistsAsync(@firstLock));
        }

        [Fact]
        public async Task Lock_WhenDisposing_ReleasesLock()
        {
            // Arrange
            var @lock = await this.lockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Act
            await @lock.DisposeAsync();

            // Assert
            Assert.False(await this.LockExistsAsync(@lock));
        }

        [Fact]
        public async Task Lock_WhenDisposingAfterLockExpired_DoesNothing()
        {
            // Arrange
            var @lock = await this.lockSource.AcquireAsync(RandomKey, ShortTtl);
            await Task.Delay(ShortTtl + TimeSpan.FromMilliseconds(100));
            Assert.False(await this.LockExistsAsync(@lock));

            // Act
            await @lock.DisposeAsync();

            // Assert
            Assert.False(await this.LockExistsAsync(@lock));
        }

        [Fact]
        public async Task Lock_WhenDisposingAfterNewLockWithSameKeyWasAcquired_OnlyNewLockExists()
        {
            // Arrange
            var @firstLock = await this.lockSource.AcquireAsync(RandomKey, ShortTtl);
            await using var @secondLock = await this.lockSource.AcquireAsync(@firstLock.Key, DefaultTtl);
            Assert.False(await this.LockExistsAsync(@firstLock));

            // Act
            await @firstLock.DisposeAsync();

            // Assert
            Assert.False(await this.LockExistsAsync(@firstLock));
            Assert.True(await this.LockExistsAsync(@secondLock));
        }

        private async Task AssertTableExistsAsync(string tableName)
        {
            Assert.True(await this.cluster.RefreshSchemaAsync());
            Assert.NotNull(this.cluster.Metadata.GetTable(TestKeyspace, tableName));
        }

        private async Task DeleteTableIfExistsAsync(string tableName)
        {
            await this.session.ExecuteAsync(new SimpleStatement($"drop table if exists {tableName}"));
            Assert.True(await this.cluster.RefreshSchemaAsync());
            Assert.Null(this.cluster.Metadata.GetTable(TestKeyspace, tableName));
        }
        
        private Task<bool> LockExistsAsync(Lock @lock)
        {
            return LockExistsAsync(@lock.Key, @lock.Id);
        }

        private async Task<bool> LockExistsAsync(string key, Guid? id = null)
        {
            var cql = $"select * from {LocksTable} where lock_key = '{key}'";

            if (id.HasValue)
            {
                cql += $" and lock_id = {id} allow filtering";
            }

            var results = await this.session.ExecuteAsync(new SimpleStatement(cql));

            return results.Any();
        }
    }
}
