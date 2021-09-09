namespace LockDotNet.MongoDb
{
    using System;
    using MongoDB.Driver;
    using MongoDB.Bson;
    using MongoDB.Bson.Serialization.Attributes;

    internal class LockEntity
    {
        [BsonId]
        [BsonRepresentation(BsonType.String)]
        public Guid LockId { get; set; }

        public string LockKey { get; set; }
    }
}