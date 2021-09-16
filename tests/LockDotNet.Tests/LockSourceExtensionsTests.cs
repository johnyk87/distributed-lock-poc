namespace LockDotNet.Tests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Moq;
    using Xunit;

    public class LockSourceExtensionsTests
    {
        private const string LockKey = "test-key";
        private static readonly TimeSpan LockTtl = TimeSpan.FromSeconds(1);

        private readonly Mock<ILockSource> lockSourceMock;

        public LockSourceExtensionsTests()
        {
            this.lockSourceMock = new Mock<ILockSource>();
        }

        [Fact]
        public async Task AttemptAcquireAsync_WithNullLockSource_ThrowsArgumentNullException()
        {
            // Arrange
            var nullLockSource = default(ILockSource);

            // Act & Assert
            await Assert.ThrowsAsync<ArgumentNullException>(() => nullLockSource.AttemptAcquireAsync(LockKey, LockTtl));
        }

        [Fact]
        public async Task AttemptAcquireAsync_WithSuccessfulAcquire_ReturnsAcquiredLock()
        {
            // Arrange
            await using var expectedLock = new Lock(LockKey, Guid.NewGuid(), _ => Task.CompletedTask);
            this.lockSourceMock
                .Setup(mock => mock.AcquireAsync(LockKey, LockTtl, It.Is<CancellationToken>(ct => ct.IsCancellationRequested)))
                .ReturnsAsync(expectedLock);

            // Act
            var lockAttempt = await this.lockSourceMock.Object.AttemptAcquireAsync(LockKey, LockTtl);

            // Assert
            Assert.NotNull(lockAttempt);
            Assert.True(lockAttempt.WasSuccessful);
            Assert.Equal(expectedLock, lockAttempt.Lock);
        }

        [Fact]
        public async Task AttemptAcquireAsync_WithOperationCanceledExceptionInAcquire_ReturnsUnsuccessfulLockAttempt()
        {
            // Arrange
            this.lockSourceMock
                .Setup(mock => mock.AcquireAsync(LockKey, LockTtl, It.Is<CancellationToken>(ct => ct.IsCancellationRequested)))
                .ThrowsAsync(new OperationCanceledException());

            // Act
            var lockAttempt = await this.lockSourceMock.Object.AttemptAcquireAsync(LockKey, LockTtl);

            // Assert
            Assert.NotNull(lockAttempt);
            Assert.False(lockAttempt.WasSuccessful);
            Assert.Null(lockAttempt.Lock);
        }

        [Fact]
        public async Task AttemptAcquireAsync_WithArgumentExceptionInAcquire_ThrowsUnderlyingException()
        {
            // Arrange
            var expectedException = new ArgumentException();
            this.lockSourceMock
                .Setup(mock => mock.AcquireAsync(LockKey, LockTtl, It.Is<CancellationToken>(ct => ct.IsCancellationRequested)))
                .ThrowsAsync(expectedException);

            // Act
            var actualException = await Assert.ThrowsAnyAsync<Exception>(
                () => this.lockSourceMock.Object.AttemptAcquireAsync(LockKey, LockTtl));

            // Assert
            Assert.Equal(expectedException, actualException);
        }
    }
}