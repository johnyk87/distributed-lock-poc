namespace LockDotNet.Tests
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Xunit;

    public abstract class BaseLockSourceTests
    {
        protected static readonly TimeSpan DefaultTtl = TimeSpan.FromSeconds(10);
        // Warning: some lock sources may only support precision to the second, so don't use milliseconds for a short TTL.
        protected static readonly TimeSpan ShortTtl = TimeSpan.FromSeconds(1);

        protected static string RandomKey => Guid.NewGuid().ToString();

        protected BaseLockSourceTests(ILockSource lockSource)
        {
            this.LockSource = lockSource ?? throw new ArgumentNullException(nameof(lockSource));
        }

        protected ILockSource LockSource { get; }

        [Fact]
        public async Task AcquireAsync_WithNoLockForTheSameKey_ReturnsValidLockInstance()
        {
            // Arrange
            var lockKey = RandomKey;

            // Act
            await using var acquiredLock = await this.LockSource.AcquireAsync(lockKey, DefaultTtl);

            // Assert
            Assert.NotNull(acquiredLock);
            Assert.Equal(lockKey, acquiredLock.Key);
            Assert.NotEqual(Guid.Empty, acquiredLock.Id);
        }

        [Fact]
        public async Task AcquireAsync_WithNoLockForTheSameKey_LockIsCreated()
        {
            // Act
            await using var acquiredLock = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Assert
            await this.AssertLockExistsAsync(acquiredLock);
        }

        [Fact]
        public async Task AcquireAsync_WithExistingLockForTheSameKey_AcquiresLockAfterExistingLockIsReleased()
        {
            // Arrange
            var existingLock = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Act
            var acquireLockTask = this.LockSource.AcquireAsync(existingLock.Key, DefaultTtl);
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            Assert.False(
                acquireLockTask.IsCompleted,
                $"Expected acquire task to still be running, but it is complete with a status of {acquireLockTask.Status}.");

            await existingLock.DisposeAsync();
            var acquiredLock = await acquireLockTask;

            // Assert
            await this.AssertLockExistsAsync(acquiredLock);
        }

        [Fact]
        public async Task AcquireAsync_WithTwoLocksWithDistinctKeys_BothLocksAreAcquired()
        {
            // Act
            await using var acquiredLock1 = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);
            await using var acquiredLock2 = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Assert
            await this.AssertLockExistsAsync(acquiredLock1);
            await this.AssertLockExistsAsync(acquiredLock2);
        }

        [Fact]
        public async Task AcquireAsync_WithConcurrentCallsForSameKey_OnlyOneCallAcquiresTheLock()
        {
            // Arrange
            var key = RandomKey;
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

            // Act
            var tasks = Enumerable
                .Range(1, 10)
                .Select(_ => Task.Run(() => this.LockSource.AcquireAsync(key, DefaultTtl, cts.Token)))
                .ToList();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Task.WhenAll(tasks));

            // Assert
            var successfulTask = Assert.Single(tasks.Where(t => t.IsCompletedSuccessfully));
            var acquiredLock = await successfulTask;
            await this.AssertLockExistsAsync(acquiredLock);
        }

        [Fact]
        public async Task AcquireAsync_WithCanceledToken_ThrowsOperationCanceledExceptionAndDoesNotAcquireLock()
        {
            // Arrange
            var key = RandomKey;
            var ct = new CancellationToken(true);

            // Act
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => this.LockSource.AcquireAsync(key, DefaultTtl, ct));

            // Assert
            await this.AssertLockDoesNotExistAsync(key);
        }

        [Fact]
        public async Task AcquireAsync_CancellingWhileWaitingForExistingLockToBeReleased_ThrowsOperationCanceledExceptionAndExistingLockSurvives()
        {
            // Arrange
            await using var existingLock = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);

            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

            // Act
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => this.LockSource.AcquireAsync(existingLock.Key, DefaultTtl, cts.Token));

            // Assert
            await this.AssertLockExistsAsync(existingLock);
        }

        [Fact]
        public async Task Lock_WhenDisposing_ReleasesLock()
        {
            // Arrange
            var acquiredLock = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Act
            await acquiredLock.DisposeAsync();

            // Assert
            await this.AssertLockDoesNotExistAsync(acquiredLock);
        }

        [Fact]
        public async Task Lock_AfterItExpires_LockNoLongerExists()
        {
            // Act
            var expiredLock = await this.SetupExpiredLockAsync();

            // Assert
            await this.AssertLockDoesNotExistAsync(expiredLock);
        }

        [Fact]
        public async Task Lock_WhenDisposingAfterLockExpired_DoesNothing()
        {
            // Arrange
            var expiredLock = await this.SetupExpiredLockAsync();

            // Act
            await expiredLock.DisposeAsync();

            // Assert
            await this.AssertLockDoesNotExistAsync(expiredLock);
        }

        [Fact]
        public async Task Lock_WhenDisposingAfterNewLockWithSameKeyWasAcquired_OnlyNewLockExists()
        {
            // Arrange
            var previousLock = await this.LockSource.AcquireAsync(RandomKey, ShortTtl);
            await using var newLock = await this.LockSource.AcquireAsync(previousLock.Key, DefaultTtl);

            // Act
            await previousLock.DisposeAsync();

            // Assert
            await this.AssertLockDoesNotExistAsync(previousLock);
            await this.AssertLockExistsAsync(newLock);
        }
        
        protected abstract Task<bool> LockExistsAsync(string key, Guid? id = null);
        
        private Task<bool> LockExistsAsync(Lock @lock)
        {
            return LockExistsAsync(@lock.Key, @lock.Id);
        }

        private async Task AssertLockExistsAsync(Lock @lock, string customMessage = null)
        {
            Assert.True(
                await this.LockExistsAsync(@lock),
                customMessage ?? $"Expected lock {{ Key = {@lock.Key}, Id = {@lock.Id}}} to exist, but it was not found.");
        }

        private async Task AssertLockDoesNotExistAsync(string key, string customMessage = null)
        {
            Assert.False(
                await this.LockExistsAsync(key),
                customMessage ?? $"Did not expect locks with key = {key} to exist, but at least one lock with that key was found.");
        }

        private async Task AssertLockDoesNotExistAsync(Lock @lock, string customMessage = null)
        {
            Assert.False(
                await this.LockExistsAsync(@lock),
                customMessage ?? $"Did not expect lock {{ Key = {@lock.Key}, Id = {@lock.Id}}} to exist, but it was found.");
        }

        private async Task<Lock> SetupExpiredLockAsync()
        {
            var @lock = await this.LockSource.AcquireAsync(RandomKey, ShortTtl);

            await Task.Delay(ShortTtl + TimeSpan.FromMilliseconds(100));

            return @lock;
        }
    }
}
