namespace LockDotNet
{
    public class LockAttempt
    {
        public LockAttempt(Lock @lock)
        {
            this.Lock = @lock;
        }

        public bool WasSuccessful => this.Lock != null;

        public Lock Lock { get; }
    }
}