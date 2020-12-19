using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace DotNetCore.CAP.Sqlite.Test
{
    [Collection("SqliteMonitoringApi")]
    public class SqliteMonitoringApiTests : DatabaseTestHost
    {
        protected override string DataBaseName => @".\DotNetCore.CAP.Sqlite.Test.Monitoring.db";
        private readonly IMonitoringApi _monitoring;
        private readonly ISerializer _serializer;

        public SqliteMonitoringApiTests()
        {
            _serializer = GetRequiredService<ISerializer>();
            var storage = GetRequiredService<IDataStorage>();
            _monitoring = storage.GetMonitoringApi();
            Initialize(storage);
        }

        private void Initialize(IDataStorage storage)
        {
            _publishedMessageId = SnowflakeId.Default().NextId();
            var publishMessage = storage.StoreMessage("test.publish.message", new Message(
                new Dictionary<string, string>()
                {
                    [Headers.MessageId] = _publishedMessageId.ToString(),
                    ["test-header"] = "test-value"
                }, null));
            storage.ChangePublishStateAsync(publishMessage, Internal.StatusName.Succeeded);
            
            var receivedMessage = storage.StoreReceivedMessage("test.received.message", "test.group", new Message(
                new Dictionary<string, string>()
                {
                    [Headers.MessageId] = SnowflakeId.Default().NextId().ToString(),
                    ["test-header"] = "test-value"
                }, null));
            _receivedMessageId = long.Parse(receivedMessage.DbId);
            storage.ChangeReceiveStateAsync(receivedMessage, Internal.StatusName.Failed);
        }

        [Fact]
        public void Messages_Test()
        {
            var normalMessageDto = new MessageQueryDto
            {
                StatusName = nameof(Internal.StatusName.Succeeded),
                MessageType = MessageType.Publish,
                CurrentPage = 0,
                PageSize = 10
            };
            var normalMessags = _monitoring.Messages(normalMessageDto);

            var lowercaseMessageDto = new MessageQueryDto
            {
                StatusName = nameof(Internal.StatusName.Succeeded).ToLower(),
                MessageType = MessageType.Publish,
                CurrentPage = 0,
                PageSize = 10
            };
            var lowercaseMessags = _monitoring.Messages(lowercaseMessageDto);

            var uppercaseMessageDto = new MessageQueryDto
            {
                StatusName = nameof(Internal.StatusName.Succeeded).ToUpper(),
                MessageType = MessageType.Publish,
                CurrentPage = 0,
                PageSize = 10
            };
            var uppercaseMessags = _monitoring.Messages(uppercaseMessageDto);

            Assert.Equal(1, normalMessags.Count);
            Assert.Equal(1, lowercaseMessags.Count);
            Assert.Equal(1, uppercaseMessags.Count);
        }

        private long _publishedMessageId;
        private long _receivedMessageId;

        [Fact]
        public async Task Get_Published_Message_Test()
        {
            var message = await _monitoring.GetPublishedMessageAsync(_publishedMessageId);
            message.Origin = _serializer.Deserialize(message.Content);
            var headerExists = message.Origin.Headers.ContainsKey("test-header");
            Assert.True(headerExists);
            Assert.Equal("test-value", message.Origin.Headers["test-header"]);
        }

        [Fact]
        public async Task Get_Received_Message_Test()
        {
            var message = await _monitoring.GetReceivedMessageAsync(_receivedMessageId);
            message.Origin = _serializer.Deserialize(message.Content);
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

        [Fact]
        public void Hourly_Failed_Jobs_Test()
        {
            var failedPublishJobs = _monitoring.HourlyFailedJobs(MessageType.Publish);
            var failedReceivedJobs = _monitoring.HourlyFailedJobs(MessageType.Subscribe);

            Assert.Equal(0, failedPublishJobs.Values.Sum());
            Assert.Equal(1, failedReceivedJobs.Values.Sum());
        }

        [Fact]
        public void Hourly_Succeeded_Jobs_Test()
        {
            var successedPublishJobs = _monitoring.HourlySucceededJobs(MessageType.Publish);
            var successedReceivedJobs = _monitoring.HourlySucceededJobs(MessageType.Subscribe);

            Assert.Equal(0, successedReceivedJobs.Values.Sum());
            Assert.Equal(1, successedPublishJobs.Values.Sum());
        }
    }
}
