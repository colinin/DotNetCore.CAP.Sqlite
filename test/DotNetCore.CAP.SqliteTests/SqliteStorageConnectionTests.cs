using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using DotNetCore.CAP.Sqlite.Test;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace DotNetCore.CAP.Sqlite.Tests
{
    [Collection("SqliteStorageConnection")]
    public class SqliteStorageConnectionTests : DatabaseTestHost
    {
        protected override string DataBaseName => @".\DotNetCore.CAP.Sqlite.Test.StorageConnection.db";
        private readonly IStorageInitializer _initializer;
        private readonly ISnowflakeId _snowflakeId;
        private readonly SqliteDataStorage _storage;
        public SqliteStorageConnectionTests()
        {
            _snowflakeId = GetRequiredService<ISnowflakeId>();
            _initializer = GetRequiredService<IStorageInitializer>();
            var serializer = GetRequiredService<ISerializer>();
            _storage = new SqliteDataStorage(SqliteOptions, CapOptions, _initializer, serializer, _snowflakeId);
        }

        [Fact]
        public async Task Storage_Lock_Test()
        {
            var lockKey = "publish_retry_v1";
            var instance = "test_instance";
            
            var lockFirst = await _storage.AcquireLockAsync(lockKey, TimeSpan.FromSeconds(5), instance);
            Assert.True(lockFirst);

            var lockTwo = await _storage.AcquireLockAsync(lockKey, TimeSpan.FromSeconds(5), instance);
            Assert.False(lockTwo);

            await Task.Delay(5100);

            var lockTree = await _storage.AcquireLockAsync(lockKey, TimeSpan.FromSeconds(5), instance);
            Assert.True(lockTree);

            await _storage.ReleaseLockAsync(lockKey, instance);
        }

        [Fact]
        public async Task Storage_New_Lock_Test()
        {
            var lockKey = "publish_retry_v1";
            var instance = "test_instance";

            var lockFirst = await _storage.AcquireLockAsync(lockKey, TimeSpan.FromSeconds(5), instance);
            Assert.True(lockFirst);

            await Task.Delay(5000);

            await _storage.RenewLockAsync(lockKey, TimeSpan.FromSeconds(6), instance);

            var lockTwo = await _storage.AcquireLockAsync(lockKey, TimeSpan.FromSeconds(5), instance);
            Assert.False(lockTwo);

            await _storage.ReleaseLockAsync(lockKey, instance);
        }

        [Fact]
        public async Task Storage_Message_Test()
        {
            var msgId = _snowflakeId.NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = await _storage.StoreMessageAsync("test.name", message);
            Assert.NotNull(mdMessage);
        }

        [Fact]
        public async Task Change_Publish_State_To_Delayed()
        {
            var msgId = _snowflakeId.NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = await _storage.StoreMessageAsync("test.delayed", message);

            await _storage.ChangePublishStateToDelayedAsync(new[] { mdMessage.DbId } );

            var messages = await _storage.GetPublishedMessagesOfNeedRetry();
            Assert.Empty(messages);
        }

        [Fact]
        public async Task Schedule_Messages_Of_Delayed()
        {
            var msgId = _snowflakeId.NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = await _storage.StoreMessageAsync("test.delayed", message);
            mdMessage.ExpiresAt = DateTime.Now.AddMilliseconds(1);
            await _storage.ChangePublishStateAsync(mdMessage, StatusName.Delayed);
            await _storage.ScheduleMessagesOfDelayedAsync(
                async (tran, messages) =>
                {
                    await Task.CompletedTask;

                    Assert.Single(messages);
                });
        }

        [Fact]
        public async Task Store_Received_Message_Test()
        {
            var msgId = _snowflakeId.NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = await _storage.StoreReceivedMessageAsync("test.name", "test.group", message);
            Assert.NotNull(mdMessage);
        }

        [Fact]
        public async Task Store_Received_Exception_Message_Test()
        {
            await _storage.StoreReceivedExceptionMessageAsync("test.name", "test.group", "");
        }

        [Fact]
        public async Task Change_Publish_State_Test()
        {
            var msgId = _snowflakeId.NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = await _storage.StoreMessageAsync("test.name", message);

            await _storage.ChangePublishStateAsync(mdMessage, StatusName.Succeeded);
        }

        [Fact]
        public async Task Change_Receive_State_Test()
        {
            var msgId = _snowflakeId.NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var mdMessage = await _storage.StoreMessageAsync("test.name", message);

            await _storage.ChangeReceiveStateAsync(mdMessage, StatusName.Succeeded);
        }

        [Fact]
        public async Task Get_Published_Messages_Of_Need_Retry_Test()
        {
            var msgId = _snowflakeId.NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            await _storage.StoreMessageAsync("test.name", message);

            var needRetryMessags = await _storage.GetPublishedMessagesOfNeedRetry();
            Assert.Single(needRetryMessags);
        }

        [Fact]
        public async Task Get_Received_Messages_Of_Need_Retry_Test()
        {
            var msgId = _snowflakeId.NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            await _storage.StoreReceivedMessageAsync("test.name", "test.group", message);

            var needRetryMessags = await _storage.GetReceivedMessagesOfNeedRetry();
            Assert.Single(needRetryMessags);
        }

        [Fact]
        public async Task Delete_Expires_Test()
        {
            var msgId = _snowflakeId.NextId().ToString();
            var header = new Dictionary<string, string>()
            {
                [Headers.MessageId] = msgId
            };
            var message = new Message(header, null);

            var publishMessage = await _storage.StoreMessageAsync("test.name", message);
            publishMessage.ExpiresAt = DateTime.Now.AddMilliseconds(-2000);
            var receivedMessage = await _storage.StoreReceivedMessageAsync("test.name", "test.group", message);
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