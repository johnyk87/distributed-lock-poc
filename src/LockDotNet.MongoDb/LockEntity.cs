namespace LockDotNet.MongoDb
{
    using System;
    using MongoDB.Bson;
    using MongoDB.Bson.Serialization.Attributes;

    internal class LockEntity
    {
        [BsonId]
        public ObjectId Id { get; set; }

        public string LockKey { get; set; }

        [BsonRepresentation(BsonType.String)]
        public Guid LockId { get; set; }

        public DateTime ExpirationDate { get; set; }
    }
}