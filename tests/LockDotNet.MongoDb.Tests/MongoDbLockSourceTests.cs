namespace LockDotNet.MongoDb.Tests
{
    using System;
    using System.Threading.Tasks;
    using LockDotNet.Tests;
    using MongoDB.Bson;
    using MongoDB.Driver;

    public class MongoDbLockSourceTests : BaseLockSourceTests
    {
        private static readonly IMongoDatabase Database = CreateDatabase();
        private static readonly IMongoCollection<BsonDocument> Collection = CreateCollection();

        public MongoDbLockSourceTests()
            : base(new MongoDbLockSource(Database))
        {
        }

        protected override async Task<bool> LockExistsAsync(string key, Guid? id = null)
        {
            var filterBuilder = Builders<BsonDocument>.Filter;
            var filter = filterBuilder.Eq("LockKey", key) & filterBuilder.Gte("ExpirationDate", DateTime.UtcNow);

            if (id.HasValue)
            {
                filter &= filterBuilder.Eq("LockId", id.ToString());
            }

            return await Collection.Find(filter).AnyAsync();
        }

        private static IMongoDatabase CreateDatabase()
        {
            var client = new MongoClient("mongodb://root:root@localhost:27017");
            return client.GetDatabase("locks_tests");
        }

        private static IMongoCollection<BsonDocument> CreateCollection()
        {
            return Database.GetCollection<BsonDocument>("locks");
        }
    }
}
