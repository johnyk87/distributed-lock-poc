namespace LockDotNet.MongoDb.Tests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using MongoDB.Bson;
    using MongoDB.Driver;
    using Xunit;

    public class MongoDbLockSourceTests
    {
        [Fact]
        public async Task AdHoc()
        {
            var client = new MongoClient("mongodb://root:root@localhost:27017");
            var database = client.GetDatabase("locks_tests");
            var collection = database.GetCollection<BsonDocument>("locks");
            
            var lockSource = new MongoDbLockSource(database);
            
            await using var @lock1 = await lockSource.AcquireAsync("test", TimeSpan.FromSeconds(2));
            var lock2Task = lockSource.AcquireAsync("test", TimeSpan.FromSeconds(2));
            await Task.Delay(1000);
            Assert.False(lock2Task.IsCompleted);
            await @lock1.DisposeAsync();
            await using var @lock2 = await lock2Task;

            var savedLock = (await collection.Find($"{{ LockKey: '{@lock1.Key}', _id: '{@lock1.Id}' }}").ToListAsync()).FirstOrDefault();
            Assert.Null(savedLock);

            savedLock = (await collection.Find($"{{ LockKey: '{@lock2.Key}', _id: '{@lock2.Id}' }}").ToListAsync()).FirstOrDefault();
            Assert.NotNull(savedLock);
        }
    }
}
