namespace DistributedLockPoc
{
    using System;
    using System.Runtime.Serialization;

    [Serializable]
    public class LockReleaseException : Exception
    {
        public LockReleaseException()
        {
        }

        public LockReleaseException(string message) : base(message)
        {
        }

        public LockReleaseException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected LockReleaseException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}