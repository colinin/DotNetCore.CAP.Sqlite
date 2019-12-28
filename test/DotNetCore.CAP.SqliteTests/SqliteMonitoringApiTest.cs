using Dapper;
using DotNetCore.CAP.Dashboard;
using DotNetCore.CAP.Dashboard.Monitoring;
using DotNetCore.CAP.Infrastructure;
using DotNetCore.CAP.Models;
using Microsoft.Extensions.Logging;
using Moq;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace DotNetCore.CAP.Sqlite.Test
{
    [Collection("Sqlite")]
    public class SqliteMonitoringApiTest : DatabaseTestHost
    {
        private readonly IMonitoringApi sqliteMonitoringApi;
        public SqliteMonitoringApiTest()
        {
            var storage = new SqliteStorage(
                  new Mock<ILogger<SqliteStorage>>().Object,
                  SqliteOptions,
                  CapOptions);

            sqliteMonitoringApi = storage.GetMonitoringApi();

            Init_PublishdMessage();
        }

        private void Init_PublishdMessage()
        {
            var sql = "INSERT INTO `cap.published`(`Id`,`Version`,`Name`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`) VALUES(@Id,'v1',@Name,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";
            var insertedId = SnowflakeId.Default().NextId();
            var publishMessage = new CapPublishedMessage
            {
                Id = insertedId,
                Name = "SqliteStorageConnectionTest",
                Content = "",
                StatusName = StatusName.Failed
            };

            using (var connection = ConnectionUtil.CreateConnection())
            {
                connection.Execute(sql, publishMessage);
            }
        }

        [Fact]
        public void Messages_Test()
        {
            var messageQueryDto = new MessageQueryDto();
            messageQueryDto.StatusName = StatusName.Failed;
            messageQueryDto.MessageType = MessageType.Publish;
            messageQueryDto.CurrentPage = 0;
            messageQueryDto.PageSize = 10;
            var messages = sqliteMonitoringApi.Messages(messageQueryDto);
            Assert.Equal(1, messages.Count);
        }

        [Fact]
        public void PublishedFailedCount_Test()
        {
            Assert.Equal(1, sqliteMonitoringApi.PublishedFailedCount());
        }
    }
}
