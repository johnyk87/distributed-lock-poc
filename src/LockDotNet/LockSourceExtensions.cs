namespace LockDotNet
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    public static class LockSourceExtensions
    {
        private static readonly CancellationToken CanceledToken = new CancellationToken(true);

        public static async Task<LockAttempt> AttemptAcquireAsync(
            this ILockSource lockSource, string lockKey, TimeSpan lockTtl)
        {
            if (lockSource is null)
            {
                throw new ArgumentNullException(nameof(lockSource));
            }

            try
            {
                 var @lock = await lockSource.AcquireAsync(lockKey, lockTtl, CanceledToken);

                 return new LockAttempt(@lock);
            }
            catch (OperationCanceledException)
            {
                return new LockAttempt(null);
            }
        }
    }
}