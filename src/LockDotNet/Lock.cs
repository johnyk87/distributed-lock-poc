namespace LockDotNet
{
    using System;
    using System.Threading.Tasks;

    public sealed class Lock : IAsyncDisposable
    {
        public delegate Task ReleaseDelegate(Lock @lock);

        private readonly ReleaseDelegate releaseDelegate;
        private bool disposed = false;

        public Lock(string key, Guid id, ReleaseDelegate releaseDelegate)
        {
            this.Key = key;
            this.Id = id;
            this.releaseDelegate = releaseDelegate ?? throw new ArgumentNullException(nameof(releaseDelegate));
        }

        public string Key { get; }

        public Guid Id { get; }

        public async ValueTask DisposeAsync()
        {
            if (this.disposed)
            {
                return;
            }

            try
            {
                await this.releaseDelegate(this);
                this.disposed = true;    
            }
            catch (Exception ex)
            {
                throw new LockReleaseException($"Failed to release lock with key \"{this.Key}\" and id \"{this.Id}\".", ex);
            }
        }
    }
}
