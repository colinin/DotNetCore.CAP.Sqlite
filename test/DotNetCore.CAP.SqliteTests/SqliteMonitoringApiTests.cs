using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace DotNetCore.CAP.Sqlite.Test
{
    [Collection("SqliteMonitoringApi")]
    public class SqliteMonitoringApiTests : DatabaseTestHost
    {
        private readonly IMonitoringApi _monitoring;
        public SqliteMonitoringApiTests()
        {
            var storage = GetRequiredService<IDataStorage>();
            _monitoring = storage.GetMonitoringApi();
            Initialize(storage);
        }

        private void Initialize(IDataStorage storage)
        {
            var publishMessage = storage.StoreMessage("test.publish.message", new Message(
                new Dictionary<string, string>()
                {
                    [Headers.MessageId] = "1000000000",
                    ["test-header"] = "test-value"
                }, null));
            storage.ChangePublishStateAsync(publishMessage, Internal.StatusName.Succeeded);
            var receivedMessage = storage.StoreReceivedMessage("test.received.message", "test.group", new Message(
                new Dictionary<string, string>()
                {
                    [Headers.MessageId] = "1000000001",
                    ["test-header"] = "test-value"
                }, null));
            _receivedMessageId = long.Parse(receivedMessage.DbId);
            storage.ChangeReceiveStateAsync(receivedMessage, Internal.StatusName.Failed);
        }

        private long _receivedMessageId;

        [Theory]
        [InlineData(1000000000)]
        public async Task Get_Published_Message_Test(long id)
        {
            var message = await _monitoring.GetPublishedMessageAsync(id);
            message.Origin = StringSerializer.DeSerialize(message.Content);
            var headerExists = message.Origin.Headers.ContainsKey("test-header");
            Assert.True(headerExists);
            Assert.Equal("test-value", message.Origin.Headers["test-header"]);
        }

        [Fact]
        public async Task Get_Received_Message_Test()
        {
            var message = await _monitoring.GetReceivedMessageAsync(_receivedMessageId);
            message.Origin = StringSerializer.DeSerialize(message.Content);
            var headerExists = message.Origin.Headers.ContainsKey("test-header");
            Assert.True(headerExists);
            Assert.Equal("test-value", message.Origin.Headers["test-header"]);
        }

        [Fact]
        public void Get_Statistics_Test()
        {
            var statistice = _monitoring.GetStatistics();
            Assert.Equal(1, statistice.PublishedSucceeded);
            Assert.Equal(0, statistice.PublishedFailed);
            Assert.Equal(0, statistice.ReceivedSucceeded);
            Assert.Equal(1, statistice.ReceivedFailed);
        }

        [Fact]
        public void Published_Failed_Count_Test()
        {
            var publishedFailedCount = _monitoring.PublishedFailedCount();
            Assert.Equal(0, publishedFailedCount);
        }

        [Fact]
        public void Published_Succeeded_Count_Test()
        {
            var publishedSucceededCount = _monitoring.PublishedSucceededCount();
            Assert.Equal(1, publishedSucceededCount);
        }

        [Fact]
        public void Received_Failed_Count_Test()
        {
            var receivedFailedCount = _monitoring.ReceivedFailedCount();
            Assert.Equal(1, receivedFailedCount);
        }

        [Fact]
        public void Received_Succeeded_Count_Test()
        {
            var receivedSucceededCount = _monitoring.ReceivedSucceededCount();
            Assert.Equal(0, receivedSucceededCount);
        }
    }
}
