namespace LockDotNet.Cassandra.Tests
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Cassandra;
    using LockDotNet.Cassandra;
    using LockDotNet.Tests;
    using Xunit;

    public class CassandraLockSourceTests : BaseLockSourceTests
    {
        private const string TestKeyspace = "locks_tests";
        private const string LocksTable = "locks";

        private static readonly ICluster Cluster = CreateCluster();
        private static readonly ISession Session = CreateSession();

        public CassandraLockSourceTests()
            : base(new CassandraLockSource(Session))
        {
        }

        [Fact]
        public void Constructor_WithNullSession_ThrowsArgumentNullException()
        {
            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => new CassandraLockSource(null));
        }

        [Fact]
        public async Task AcquireAsync_OnFirstCallAndWithoutTable_CreatesTable()
        {
            // Arrange
            await DeleteTableIfExistsAsync(LocksTable);

            var newLockSource = new CassandraLockSource(Session);

            // Act
            await newLockSource.AcquireAsync(RandomKey, DefaultTtl);

            // Assert
            await AssertTableExistsAsync(LocksTable);
        }

        protected override async Task<bool> LockExistsAsync(string key, Guid? id = null)
        {
            var cql = $"select * from {LocksTable} where lock_key = '{key}'";

            if (id.HasValue)
            {
                cql += $" and lock_id = {id} allow filtering";
            }

            var results = await Session.ExecuteAsync(new SimpleStatement(cql));

            return results.Any();
        }

        private static ICluster CreateCluster()
        {
            return global::Cassandra.Cluster.Builder()
                .AddContactPoint("127.0.0.1")
                .WithDefaultKeyspace(TestKeyspace)
                .Build();
        }

        private static ISession CreateSession()
        {
            return ((Cluster)Cluster).ConnectAndCreateDefaultKeyspaceIfNotExists();
        }

        private static async Task DeleteTableIfExistsAsync(string tableName)
        {
            await Session.ExecuteAsync(new SimpleStatement($"drop table if exists {tableName}"));
            Assert.True(await Cluster.RefreshSchemaAsync());
            Assert.Null(Cluster.Metadata.GetTable(TestKeyspace, tableName));
        }

        private static async Task AssertTableExistsAsync(string tableName)
        {
            Assert.True(await Cluster.RefreshSchemaAsync());
            Assert.NotNull(Cluster.Metadata.GetTable(TestKeyspace, tableName));
        }
    }
}
