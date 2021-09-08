namespace LockDotNet
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public interface ILockSource
    {
        Task<Lock> AcquireAsync(string lockKey, TimeSpan lockTtl, CancellationToken cancellationToken = default);
    }
}
