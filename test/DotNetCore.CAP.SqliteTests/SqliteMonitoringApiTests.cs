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
        private readonly ISnowflakeId _snowflakeId;

        public SqliteMonitoringApiTests()
        {
            _serializer = GetRequiredService<ISerializer>();
            _snowflakeId = GetRequiredService<ISnowflakeId>();
            var storage = GetRequiredService<IDataStorage>();
            _monitoring = storage.GetMonitoringApi();
            Initialize(storage);
        }

        private void Initialize(IDataStorage storage)
        {
            _publishedMessageId = _snowflakeId.NextId();
            var publishMessage = storage.StoreMessageAsync("test.publish.message", new Message(
                new Dictionary<string, string>()
                {
                    [Headers.MessageId] = _publishedMessageId.ToString(),
                    ["test-header"] = "test-value"
                }, null)).GetAwaiter().GetResult();
            storage.ChangePublishStateAsync(publishMessage, Internal.StatusName.Succeeded);
            
            var receivedMessage = storage.StoreReceivedMessageAsync("test.received.message", "test.group", new Message(
                new Dictionary<string, string>()
                {
                    [Headers.MessageId] = _snowflakeId.NextId().ToString(),
                    ["test-header"] = "test-value"
                }, null)).GetAwaiter().GetResult();
            _receivedMessageId = long.Parse(receivedMessage.DbId);
            storage.ChangeReceiveStateAsync(receivedMessage, Internal.StatusName.Failed);
        }

        [Fact]
        public async Task Messages_Test()
        {
            var normalMessageDto = new MessageQueryDto
            {
                StatusName = nameof(Internal.StatusName.Succeeded),
                MessageType = MessageType.Publish,
                CurrentPage = 0,
                PageSize = 10
            };
            var normalMessags = await _monitoring.GetMessagesAsync(normalMessageDto);

            var lowercaseMessageDto = new MessageQueryDto
            {
                StatusName = nameof(Internal.StatusName.Succeeded).ToLower(),
                MessageType = MessageType.Publish,
                CurrentPage = 0,
                PageSize = 10
            };
            var lowercaseMessags = await _monitoring.GetMessagesAsync(lowercaseMessageDto);

            var uppercaseMessageDto = new MessageQueryDto
            {
                StatusName = nameof(Internal.StatusName.Succeeded).ToUpper(),
                MessageType = MessageType.Publish,
                CurrentPage = 0,
                PageSize = 10
            };
            var uppercaseMessags = await _monitoring.GetMessagesAsync(uppercaseMessageDto);

            Assert.Equal(1, normalMessags.Items.Count);
            Assert.Equal(1, normalMessags.Totals);

            Assert.Equal(1, lowercaseMessags.Items.Count);
            Assert.Equal(1, lowercaseMessags.Totals);

            Assert.Equal(1, uppercaseMessags.Items.Count);
            Assert.Equal(1, uppercaseMessags.Totals);
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
        public async Task Get_Statistics_Test()
        {
            var statistice = await _monitoring.GetStatisticsAsync();
            Assert.Equal(1, statistice.PublishedSucceeded);
            Assert.Equal(0, statistice.PublishedFailed);
            Assert.Equal(0, statistice.ReceivedSucceeded);
            Assert.Equal(1, statistice.ReceivedFailed);
        }

        [Fact]
        public async Task Published_Failed_Count_Test()
        {
            var publishedFailedCount = await _monitoring.PublishedFailedCount();
            Assert.Equal(0, publishedFailedCount);
        }

        [Fact]
        public async Task Published_Succeeded_Count_Test()
        {
            var publishedSucceededCount = await _monitoring.PublishedSucceededCount();
            Assert.Equal(1, publishedSucceededCount);
        }

        [Fact]
        public async Task Received_Failed_Count_Test()
        {
            var receivedFailedCount = await _monitoring.ReceivedFailedCount();
            Assert.Equal(1, receivedFailedCount);
        }

        [Fact]
        public async Task Received_Succeeded_Count_Test()
        {
            var receivedSucceededCount = await _monitoring.ReceivedSucceededCount();
            Assert.Equal(0, receivedSucceededCount);
        }

        [Fact]
        public async Task Hourly_Failed_Jobs_Test()
        {
            var failedPublishJobs = await _monitoring.HourlyFailedJobs(MessageType.Publish);
            var failedReceivedJobs = await _monitoring.HourlyFailedJobs(MessageType.Subscribe);

            Assert.Equal(0, failedPublishJobs.Values.Sum());
            Assert.Equal(1, failedReceivedJobs.Values.Sum());
        }

        [Fact]
        public async Task Hourly_Succeeded_Jobs_Test()
        {
            var successedPublishJobs = await _monitoring.HourlySucceededJobs(MessageType.Publish);
            var successedReceivedJobs = await _monitoring.HourlySucceededJobs(MessageType.Subscribe);

            Assert.Equal(0, successedReceivedJobs.Values.Sum());
            Assert.Equal(1, successedPublishJobs.Values.Sum());
        }
    }
}
