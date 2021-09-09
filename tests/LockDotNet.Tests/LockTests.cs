namespace LockDotNet.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using LockDotNet;
    using Xunit;

    public class LockTests
    {
        private const string TestKey = "my-key";

        private readonly Guid TestId = Guid.NewGuid();

        [Fact]
        public void Constructor_WithNullReleaseDelegate_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new Lock(TestKey, TestId, null));
        }

        [Fact]
        public void Constructor_WithValidData_PropertiesReturnProvidedValues()
        {
            // Act
            var @lock = new Lock(TestKey, TestId, _ => Task.CompletedTask);

            // Assert
            Assert.Equal(TestKey, @lock.Key);
            Assert.Equal(TestId, @lock.Id);
        }

        [Fact]
        public async Task DisposeAsync_WhenCalled_CallsReleaseDelegate()
        {
            // Arrange
            var releaseCount = 0;

            Task ReleaseDelegate(Lock lockToRelease)
            {
                Interlocked.Increment(ref releaseCount);
                return Task.CompletedTask;
            }
            
            var @lock = new Lock(TestKey, TestId, ReleaseDelegate);

            // Act
            await @lock.DisposeAsync();

            // Assert
            Assert.Equal(1, releaseCount);
        }

        [Fact]
        public async Task DisposeAsync_WithMultipleCalls_CallsReleaseDelegateOnlyOnce()
        {
            // Arrange
            var releaseCount = 0;

            Task ReleaseDelegate(Lock lockToRelease)
            {
                Interlocked.Increment(ref releaseCount);
                return Task.CompletedTask;
            }
            
            var @lock = new Lock(TestKey, TestId, ReleaseDelegate);

            // Act
            await @lock.DisposeAsync();
            await @lock.DisposeAsync();
            await @lock.DisposeAsync();

            // Assert
            Assert.Equal(1, releaseCount);
        }

        [Fact]
        public async Task DisposeAsync_WithReleaseException_ThrowsLockReleaseExceptionWithMessageAndInnerException()
        {
            // Arrange
            var expectedErrorMessage = $"Failed to release lock with key \"{TestKey}\" and id \"{TestId}\".";
            var expectedInnerException = new Exception("Oops");
            
            var @lock = new Lock(TestKey, TestId, _ => throw expectedInnerException);

            // Act
            var actualException = await Assert.ThrowsAsync<LockReleaseException>(async () => await @lock.DisposeAsync());

            // Assert
            Assert.Equal(expectedErrorMessage, actualException.Message);
            Assert.Equal(expectedInnerException, actualException.InnerException);
        }
        

        [Fact]
        public async Task DisposeAsync_WithReleaseExceptionOnFirstTry_CanStillReleaseOnSecondAttempt()
        {
            // Arrange
            var releaseCount = 0;

            Task ReleaseDelegate(Lock lockToRelease)
            {
                if (Interlocked.Increment(ref releaseCount) == 1)
                {
                    throw new Exception();
                }

                return Task.CompletedTask;
            }
            
            var @lock = new Lock(TestKey, TestId, ReleaseDelegate);

            // Act
            await Assert.ThrowsAsync<LockReleaseException>(async () => await @lock.DisposeAsync());
            await @lock.DisposeAsync();

            // Assert
            Assert.Equal(2, releaseCount);
        }
    }
}
