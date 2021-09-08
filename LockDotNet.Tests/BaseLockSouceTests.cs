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
        protected static readonly TimeSpan ShortTtl = TimeSpan.FromSeconds(1);

        protected static string RandomKey => Guid.NewGuid().ToString();

        protected BaseLockSourceTests(ILockSource lockSource)
        {
            this.LockSource = lockSource ?? throw new ArgumentNullException(nameof(lockSource));
        }

        protected ILockSource LockSource { get; }

        [Fact]
        public async Task AcquireAsync_WithNoLockForTheSameKey_AcquiresLock()
        {
            // Arrange
            var lockKey = RandomKey;

            // Act
            await using var @lock = await this.LockSource.AcquireAsync(lockKey, DefaultTtl);

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
            var @firstLock = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Act
            var secondLockTask = this.LockSource.AcquireAsync(@firstLock.Key, DefaultTtl);
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
            var @lock = await this.LockSource.AcquireAsync(RandomKey, ShortTtl);
            await Task.Delay(ShortTtl + TimeSpan.FromMilliseconds(100));

            // Assert
            Assert.False(await this.LockExistsAsync(@lock));
        }

        [Fact]
        public async Task AcquireAsync_WithTwoLocksWithDistinctKeys_BothLocksAreAcquired()
        {
            // Act
            await using var @firstLock = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);
            await using var @secondLock = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);

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
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => this.LockSource.AcquireAsync(key, DefaultTtl, cts.Token));

            // Assert
            Assert.False(await this.LockExistsAsync(key));
        }

        [Fact]
        public async Task AcquireAsync_CancellingWhileWaitingForExistingToken_ThrowsOperationCanceledExceptionAndExistingLockSurvives()
        {
            // Arrange
            await using var @firstLock = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

            // Act
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => this.LockSource.AcquireAsync(@firstLock.Key, DefaultTtl, cts.Token));

            // Assert
            Assert.True(await this.LockExistsAsync(@firstLock));
        }

        [Fact]
        public async Task Lock_WhenDisposing_ReleasesLock()
        {
            // Arrange
            var @lock = await this.LockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Act
            await @lock.DisposeAsync();

            // Assert
            Assert.False(await this.LockExistsAsync(@lock));
        }

        [Fact]
        public async Task Lock_WhenDisposingAfterLockExpired_DoesNothing()
        {
            // Arrange
            var @lock = await this.LockSource.AcquireAsync(RandomKey, ShortTtl);
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
            var @firstLock = await this.LockSource.AcquireAsync(RandomKey, ShortTtl);
            await using var @secondLock = await this.LockSource.AcquireAsync(@firstLock.Key, DefaultTtl);
            Assert.False(await this.LockExistsAsync(@firstLock));

            // Act
            await @firstLock.DisposeAsync();

            // Assert
            Assert.False(await this.LockExistsAsync(@firstLock));
            Assert.True(await this.LockExistsAsync(@secondLock));
        }
        
        protected abstract Task<bool> LockExistsAsync(string key, Guid? id = null);
        
        private Task<bool> LockExistsAsync(Lock @lock)
        {
            return LockExistsAsync(@lock.Key, @lock.Id);
        }
    }
}
