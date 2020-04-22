using Dapper;
using System.IO;
using Xunit;

namespace DotNetCore.CAP.Sqlite.Test
{
    [Collection("Sqlite")]
    public class SqliteStorageTest : DatabaseTestHost
    {
        private readonly string _dbConnectionString;

        public SqliteStorageTest()
        {
            _dbConnectionString = ConnectionUtil.GetConnectionString();
        }

        [Fact]
        public void Database_IsExists()
        {
            var databaseName = ConnectionUtil.GetDatabaseName();
            var databaseExists = File.Exists(databaseName);
            Assert.True(databaseExists);
        }

        [Theory]
        [InlineData("cap.Published")]
        [InlineData("cap.Received")]
        public void DatabaseTable_IsExists(string tableName)
        {
            using (var connection = ConnectionUtil.CreateConnection(_dbConnectionString))
            {
                var sql = $"SELECT name FROM sqlite_master where type='table' and name='{tableName}'";
                var result = connection.QueryFirstOrDefault<string>(sql);
                Assert.NotNull(result);
                Assert.Equal(tableName, result);
            }
        }
    }
}