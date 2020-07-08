// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Dapper;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DotNetCore.CAP.Sqlite
{
    internal class SqliteMonitoringApi : IMonitoringApi
    {
        private readonly IOptions<SqliteOptions> _options;
        private readonly string _published;
        private readonly string _received;

        public SqliteMonitoringApi(
            IOptions<SqliteOptions> options, 
            IStorageInitializer initializer)
        {
            _options = options;
            _published = initializer.GetPublishedTableName();
            _received = initializer.GetReceivedTableName();
        }

        public async Task<MediumMessage> GetPublishedMessageAsync(long id)
        {
            var sql = $@"SELECT `Id` as DbId, `Content`,`Added`,`ExpiresAt`,`Retries` FROM `{_published}` WHERE `Id`=@Id;";
            var sqlParam = new { Id = id };
            using (var connection = new SqliteConnection(_options.Value.ConnectionString))
            {
                return await connection.QueryFirstOrDefaultAsync<MediumMessage>(sql, sqlParam);
            }
        }

        public async Task<MediumMessage> GetReceivedMessageAsync(long id)
        {
            var sql = $@"SELECT `Id` as DbId, `Content`,`Added`,`ExpiresAt`,`Retries` FROM `{_received}` WHERE Id=@Id;";
            var sqlParam = new { Id = id };
            using (var connection = new SqliteConnection(_options.Value.ConnectionString))
            {
                return await connection.QueryFirstOrDefaultAsync<MediumMessage>(sql, sqlParam);
            }
        }

        public StatisticsDto GetStatistics()
        {
            var sqlBuilder = new StringBuilder();
            sqlBuilder.AppendLine("PRAGMA read_uncommitted = 1;")
                .AppendFormat("select count(`Id`) from `{0}` where `StatusName` = 'Succeeded';", _published)
                .AppendLine()
                .AppendFormat("select count(`Id`) from `{0}` where `StatusName` = 'Succeeded';", _received)
                .AppendLine()
                .AppendFormat("select count(`Id`) from `{0}` where `StatusName` = 'Failed';", _published)
                .AppendLine()
                .AppendFormat("select count(`Id`) from `{0}` where `StatusName` = 'Failed';", _received);

            var statistics = UseConnection(connection =>
            {
                var stats = new StatisticsDto();
                using (var multi = connection.QueryMultiple(sqlBuilder.ToString()))
                {
                    stats.PublishedSucceeded = multi.ReadSingle<int>();
                    stats.ReceivedSucceeded = multi.ReadSingle<int>();

                    stats.PublishedFailed = multi.ReadSingle<int>();
                    stats.ReceivedFailed = multi.ReadSingle<int>();
                }

                return stats;
            });
            return statistics;
        }

        public IDictionary<DateTime, int> HourlyFailedJobs(MessageType type)
        {
            var tableName = type == MessageType.Publish ? _published : _received;
            return UseConnection(connection =>
                GetHourlyTimelineStats(connection, tableName, nameof(StatusName.Failed)));
        }

        public IDictionary<DateTime, int> HourlySucceededJobs(MessageType type)
        {
            var tableName = type == MessageType.Publish ? _published : _received;
            return UseConnection(connection =>
                GetHourlyTimelineStats(connection, tableName, nameof(StatusName.Succeeded)));
        }

        public IList<MessageDto> Messages(MessageQueryDto queryDto)
        {
            var tableName = queryDto.MessageType == MessageType.Publish ? _published : _received;
            var where = string.Empty;
            if (!string.IsNullOrEmpty(queryDto.StatusName))
            {
                where += " and `StatusName` = @StatusName";
            }

            if (!string.IsNullOrEmpty(queryDto.Name))
            {
                where += " and `Name` = @Name";
            }

            if (!string.IsNullOrEmpty(queryDto.Group))
            {
                where += " and `Group` = @Group";
            }

            if (!string.IsNullOrEmpty(queryDto.Content))
            {
                where += " and `Content` like @Content";
            }

            var sqlQuery =
                $"select * from `{tableName}` where 1=1 {where} order by `Added` desc limit @Offset,@Limit";

            return UseConnection(conn => conn.Query<MessageDto>(sqlQuery, new
            {
                queryDto.StatusName,
                queryDto.Group,
                queryDto.Name,
                Content = $"%{queryDto.Content}%",//参数化Like查询的一个错误
                Offset = queryDto.CurrentPage * queryDto.PageSize,
                Limit = queryDto.PageSize
            }).ToList());
        }

        public int PublishedFailedCount()
        {
            return UseConnection(conn => GetNumberOfMessage(conn, _published, nameof(StatusName.Failed)));
        }

        public int PublishedSucceededCount()
        {
            return UseConnection(conn => GetNumberOfMessage(conn, _published, nameof(StatusName.Succeeded)));
        }

        public int ReceivedFailedCount()
        {
            return UseConnection(conn => GetNumberOfMessage(conn, _received, nameof(StatusName.Failed)));
        }

        public int ReceivedSucceededCount()
        {
            return UseConnection(conn => GetNumberOfMessage(conn, _received, nameof(StatusName.Succeeded)));
        }

        private int GetNumberOfMessage(IDbConnection connection, string tableName, string statusName)
        {
            var sqlQuery = $"select count(`Id`) from `{tableName}` where `StatusName` = @state";

            var count = connection.ExecuteScalar<int>(sqlQuery, new { state = statusName });
            return count;
        }

        private T UseConnection<T>(Func<IDbConnection, T> action)
        {
            return action(new SqliteConnection(_options.Value.ConnectionString));
        }

        private Dictionary<DateTime, int> GetHourlyTimelineStats(IDbConnection connection, string tableName,
            string statusName)
        {
            var endDate = DateTime.Now;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keyMaps = dates.ToDictionary(x => x.ToString("yyyy-MM-dd-HH"), x => x);

            return GetTimelineStats(connection, tableName, statusName, keyMaps);
        }

        private Dictionary<DateTime, int> GetTimelineStats(
            IDbConnection connection,
            string tableName,
            string statusName,
            IDictionary<string, DateTime> keyMaps)
        {
            var sqlQuery =
                $@"
        select aggr.* from (
            select strftime('%Y-%m-%d-%H', `Added`) as Key,
                count(`id`) as Count
            from  `{tableName}`
            where `StatusName` = @statusName
            group by strftime('%Y-%m-%d-%H', `Added`)
        ) as aggr where aggr.`Key` in @keys;";

            var valuesMap = connection.Query<TimelineCounter>(
                    sqlQuery,
                    new { keys = keyMaps.Keys, statusName })
                .ToDictionary(x => x.Key, x => x.Count);

            foreach (var key in keyMaps.Keys)
            {
                if (!valuesMap.ContainsKey(key))
                {
                    valuesMap.Add(key, 0);
                }
            }

            var result = new Dictionary<DateTime, int>();
            for (var i = 0; i < keyMaps.Count; i++)
            {
                var value = valuesMap[keyMaps.ElementAt(i).Key];
                result.Add(keyMaps.ElementAt(i).Value, value);
            }

            return result;
        }

        class TimelineCounter
        {
            public string Key { get; set; }
            public int Count { get; set; }
        }
    }
}