using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Sqlite.Test;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace DotNetCore.CAP.Sqlite.Tests
{
    [Collection("SqliteStorageConnection")]
    public class SqliteStorageConnectionTests : DatabaseTestHost
    {
        protected override string DataBaseName => @".\DotNetCore.CAP.Sqlite.Test.StorageConnection.db";
        private readonly IStorageInitializer _initializer;
        private readonly SqliteDataStorage _storage;
        public SqliteStorageConnectionTests()
        {
            _initializer = GetRequiredService<IStorageInitializer>();
            _storage = new SqliteDataStorage(SqliteOptions, CapOptions, _initializer);
        }

        [Fact]
        public void Storage_Message_Test()
        {
            var msgId = SnowflakeId.Default().NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = _storage.StoreMessage("test.name", message);
            Assert.NotNull(mdMessage);
        }

        [Fact]
        public void Store_Received_Message_Test()
        {
            var msgId = SnowflakeId.Default().NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = _storage.StoreReceivedMessage("test.name", "test.group", message);
            Assert.NotNull(mdMessage);
        }

        [Fact]
        public void Store_Received_Exception_Message_Test()
        {
            _storage.StoreReceivedExceptionMessage("test.name", "test.group", "");
        }

        [Fact]
        public async Task Change_Publish_State_Test()
        {
            var msgId = SnowflakeId.Default().NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = _storage.StoreMessage("test.name", message);

            await _storage.ChangePublishStateAsync(mdMessage, StatusName.Succeeded);
        }

        [Fact]
        public async Task Change_Receive_State_Test()
        {
            var msgId = SnowflakeId.Default().NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = _storage.StoreMessage("test.name", message);

            await _storage.ChangeReceiveStateAsync(mdMessage, StatusName.Succeeded);
        }

        [Fact]
        public async Task Get_Published_Messages_Of_Need_Retry_Test()
        {
            var msgId = SnowflakeId.Default().NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            _storage.StoreMessage("test.name", message);

            var needRetryMessags = await _storage.GetPublishedMessagesOfNeedRetry();
            Assert.Single(needRetryMessags);
        }

        [Fact]
        public async Task Get_Received_Messages_Of_Need_Retry_Test()
        {
            var msgId = SnowflakeId.Default().NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            _storage.StoreReceivedMessage("test.name", "test.group", message);

            var needRetryMessags = await _storage.GetReceivedMessagesOfNeedRetry();
            Assert.Single(needRetryMessags);
        }

        [Fact]
        public async Task Delete_Expires_Test()
        {
            var msgId = SnowflakeId.Default().NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var publishMessage = _storage.StoreMessage("test.name", message);
            publishMessage.ExpiresAt = DateTime.Now.AddMilliseconds(-2000);
            var receivedMessage = _storage.StoreReceivedMessage("test.name", "test.group", message);
            receivedMessage.ExpiresAt = DateTime.Now.AddMilliseconds(-2000);

            await _storage.ChangePublishStateAsync(publishMessage, StatusName.Succeeded);
            await _storage.ChangeReceiveStateAsync(receivedMessage, StatusName.Succeeded);

            var delPublishMessageCount = await _storage
                .DeleteExpiresAsync(_initializer.GetPublishedTableName(), DateTime.Now);

            var delReceivedMessageCount = await _storage
                .DeleteExpiresAsync(_initializer.GetReceivedTableName(), DateTime.Now);

            Assert.Equal(1, delPublishMessageCount);
            Assert.Equal(1, delReceivedMessageCount);
        }
    }
}