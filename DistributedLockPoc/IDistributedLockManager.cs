namespace DistributedLockPoc
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IDistributedLockManager
    {
        Task<DistributedLock> AcquireAsync(
            string lockKey, TimeSpan lockTtl, TimeSpan retryInterval, CancellationToken cancellationToken = default);

        Task<bool> ReleaseAsync(DistributedLock distributedLock);
    }
}
