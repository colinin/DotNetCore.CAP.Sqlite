using Dapper;
using DotNetCore.CAP.Infrastructure;
using DotNetCore.CAP.Models;
using DotNetCore.CAP.Sqlite.Test;
using System;
using System.Threading.Tasks;
using Xunit;

namespace DotNetCore.CAP.Sqlite.Tests
{
    [Collection("Sqlite")]
    public class SqliteStorageConnectionTests : DatabaseTestHost
    {
        private readonly SqliteStorageConnection _storage;
        public SqliteStorageConnectionTests()
        {
            _storage = new SqliteStorageConnection(SqliteOptions, CapOptions);
        }

        [Fact]
        public async Task GetPublishedMessageAsync_Test()
        {
            var sql = "INSERT INTO `cap.published`(`Id`,`Version`,`Name`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`) VALUES(@Id,'v1',@Name,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";
            var insertedId = SnowflakeId.Default().NextId();
            var publishMessage = new CapPublishedMessage
            {
                Id = insertedId,
                Name = "SqliteStorageConnectionTest",
                Content = "",
                StatusName = StatusName.Scheduled
            };

            using (var connection = ConnectionUtil.CreateConnection())
            {
                await connection.ExecuteAsync(sql, publishMessage);
            }

            var message = await _storage.GetPublishedMessageAsync(insertedId);
            Assert.NotNull(message);
            Assert.Equal("SqliteStorageConnectionTest", message.Name);
            Assert.Equal(StatusName.Scheduled, message.StatusName);
        }

        [Fact]
        public void StoreReceivedMessageAsync_Test()
        {
            var receivedMessage = new CapReceivedMessage
            {
                Name = "SqliteStorageConnectionTest",
                Content = "",
                Group = "mygroup",
                StatusName = StatusName.Scheduled
            };

            Exception exception = null;
            try
            {
                _storage.StoreReceivedMessage(receivedMessage);
            }
            catch (Exception ex)
            {
                exception = ex;
            }
            Assert.Null(exception);
        }

        [Fact]
        public async Task GetReceivedMessageAsync_Test()
        {
            var sql = @"INSERT INTO `cap.received`(`Id`,`Version`,`Name`,`Group`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`) VALUES(@Id,'v1',@Name,@Group,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";
            var insertedId = SnowflakeId.Default().NextId();
            var receivedMessage = new CapReceivedMessage
            {
                Id = insertedId,
                Name = "SqliteStorageConnectionTest",
                Content = "",
                Group = "mygroup",
                StatusName = StatusName.Scheduled
            };

            using (var connection = ConnectionUtil.CreateConnection())
            {
                await connection.ExecuteAsync(sql, receivedMessage);
            }

            var message = await _storage.GetReceivedMessageAsync(insertedId);

            Assert.NotNull(message);
            Assert.Equal(StatusName.Scheduled, message.StatusName);
            Assert.Equal("SqliteStorageConnectionTest", message.Name);
            Assert.Equal("mygroup", message.Group);
        }
    }
}