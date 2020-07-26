namespace DistributedLockPoc
{
    using System;
    using System.Threading.Tasks;

    public sealed class DistributedLock : IAsyncDisposable
    {
        private readonly IDistributedLockManager lockManager;

        private bool disposed = false;

        public DistributedLock(IDistributedLockManager lockManager, string key, Guid id)
        {
            this.lockManager = lockManager;
            this.Key = key;
            this.Id = id;
        }

        public string Key { get; }

        public Guid Id { get; }

        public async ValueTask DisposeAsync()
        {
            if (disposed)
            {
                return;
            }

            await this.lockManager.ReleaseAsync(this);
        }
    }
}
