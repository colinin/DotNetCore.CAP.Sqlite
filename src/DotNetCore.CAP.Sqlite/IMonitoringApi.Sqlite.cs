// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Dapper;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DotNetCore.CAP.Sqlite;

internal class SqliteMonitoringApi : IMonitoringApi
{
    private readonly IOptions<SqliteOptions> _options;
    private readonly string _published;
    private readonly string _received;
    private readonly ISerializer _serializer;

    public SqliteMonitoringApi(
        IOptions<SqliteOptions> options, 
        IStorageInitializer initializer,
        ISerializer serializer)
    {
        _options = options;
        _published = initializer.GetPublishedTableName();
        _received = initializer.GetReceivedTableName();
        _serializer = serializer;
    }

    public async virtual Task<MediumMessage> GetPublishedMessageAsync(long id)
    {
        return await GetMessageAsync(_published, id);
    }

    public async virtual Task<MediumMessage> GetReceivedMessageAsync(long id)
    {
        return await GetMessageAsync(_received, id);
    }

    public async virtual Task<StatisticsDto> GetStatisticsAsync()
    {
        var sqlBuilder = new StringBuilder();
        sqlBuilder.AppendLine("PRAGMA READ_UNCOMMITTED = 1;")
            .AppendFormat("SELECT COUNT(`Id`) FROM `{0}` WHERE `StatusName` = 'Succeeded';", _published)
            .AppendLine()
            .AppendFormat("SELECT COUNT(`Id`) FROM `{0}` WHERE `StatusName` = 'Succeeded';", _received)
            .AppendLine()
            .AppendFormat("SELECT COUNT(`Id`) FROM `{0}` WHERE `StatusName` = 'Failed';", _published)
            .AppendLine()
            .AppendFormat("SELECT COUNT(`Id`) FROM `{0}` WHERE `StatusName` = 'Failed';", _received);

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);

        var multi = await connection.QueryMultipleAsync(sqlBuilder.ToString());

        var statistics = new StatisticsDto
        {
            PublishedSucceeded = multi.ReadSingle<int>(),
            ReceivedSucceeded = multi.ReadSingle<int>(),

            PublishedFailed = multi.ReadSingle<int>(),
            ReceivedFailed = multi.ReadSingle<int>()
        };

        return statistics;
    }

    public async virtual Task<IDictionary<DateTime, int>> HourlyFailedJobs(MessageType type)
    {
        var tableName = type == MessageType.Publish ? _published : _received;

        return await GetHourlyTimelineStats(tableName, nameof(StatusName.Failed));
    }

    public async virtual Task<IDictionary<DateTime, int>> HourlySucceededJobs(MessageType type)
    {
        var tableName = type == MessageType.Publish ? _published : _received;
        return await GetHourlyTimelineStats(tableName, nameof(StatusName.Succeeded));
    }

    public async virtual Task<PagedQueryResult<MessageDto>> GetMessagesAsync(MessageQueryDto queryDto)
    {
        var tableName = queryDto.MessageType == MessageType.Publish ? _published : _received;
        var where = string.Empty;
        if (!string.IsNullOrEmpty(queryDto.StatusName))
        {
            where += " AND `StatusName` = @StatusName";
        }

        if (!string.IsNullOrEmpty(queryDto.Name))
        {
            where += " AND `Name` = @Name";
        }

        if (!string.IsNullOrEmpty(queryDto.Group))
        {
            where += " AND `Group` = @Group";
        }

        if (!string.IsNullOrEmpty(queryDto.Content))
        {
            where += " AND `Content` like @Content";
        }

        var sqlParams = new
        {
            queryDto.StatusName,
            queryDto.Group,
            queryDto.Name,
            Content = $"%{queryDto.Content}%",//参数化Like查询的一个错误
            Offset = queryDto.CurrentPage * queryDto.PageSize,
            Limit = queryDto.PageSize
        };

        var sqlQuery =
            $"SELECT * FROM `{tableName}` WHERE 1=1 {where} ORDER BY `Added` DESC LIMIT @Offset,@Limit";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);

        var count = await connection.QueryFirstAsync<int>(
            $"SELECT COUNT(1) FROM `{tableName}` WHERE 1 = 1 {where}",
            sqlParams);

        var messages = (await connection.QueryAsync<MessageDto>(sqlQuery, sqlParams)).ToList();

        return new PagedQueryResult<MessageDto>
        {
            Items = messages,
            PageIndex = queryDto.CurrentPage,
            PageSize = queryDto.PageSize,
            Totals = count
        };
    }

    public virtual ValueTask<int> PublishedFailedCount()
    {
        return GetNumberOfMessage(_published, nameof(StatusName.Failed));
    }

    public virtual ValueTask<int> PublishedSucceededCount()
    {
        return GetNumberOfMessage(_published, nameof(StatusName.Succeeded));
    }

    public virtual ValueTask<int> ReceivedFailedCount()
    {
        return GetNumberOfMessage(_received, nameof(StatusName.Failed));
    }

    public virtual ValueTask<int> ReceivedSucceededCount()
    {
        return GetNumberOfMessage(_received, nameof(StatusName.Succeeded));
    }

    private async Task<MediumMessage> GetMessageAsync(string tableName, long id)
    {
        var sql = $@"SELECT `Id` as DbId, `Content`,`Added`,`ExpiresAt`,`Retries` FROM `{tableName}` WHERE `Id`=@Id;";
        var sqlParam = new { Id = id }; 
        await using var connection = new SqliteConnection(_options.Value.ConnectionString);

        var message = await connection.QueryFirstOrDefaultAsync<MediumMessage>(sql, sqlParam);

        if (!string.IsNullOrWhiteSpace(message.Content))
        {
            message.Origin = _serializer.Deserialize(message.Content);
        }

        return message;
    }

    private async ValueTask<int> GetNumberOfMessage(string tableName, string statusName)
    {
        var sqlQuery = $"SELECT COUNT(`Id`) FROM `{tableName}` WHERE `StatusName` = @state";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);

        var count = await connection.ExecuteScalarAsync<int>(sqlQuery, new { state = statusName });
        return count;
    }

    private Task<Dictionary<DateTime, int>> GetHourlyTimelineStats(string tableName, string statusName)
    {
        var endDate = DateTime.Now;
        var dates = new List<DateTime>();
        for (var i = 0; i < 24; i++)
        {
            dates.Add(endDate);
            endDate = endDate.AddHours(-1);
        }

        var keyMaps = dates.ToDictionary(x => x.ToString("yyyy-MM-dd-HH"), x => x);

        return GetTimelineStats(tableName, statusName, keyMaps);
    }

    private async Task<Dictionary<DateTime, int>> GetTimelineStats(
        string tableName,
        string statusName,
        IDictionary<string, DateTime> keyMaps)
    {
        var sqlQuery =
            $@"
        SELECT aggr.* FROM (
            SELECT STRFTIME('%Y-%m-%d-%H', `Added`) AS Key,
                COUNT(`id`) AS Count
            FROM  `{tableName}`
            WHERE `StatusName` = @statusName
            GROUP BY STRFTIME('%Y-%m-%d-%H', `Added`)
        ) AS aggr WHERE aggr.`Key` >= @minKey AND aggr.`Key` <= @maxKey;;";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        {
            var valuesMap = (await connection.QueryAsync<TimelineCounter>(
                sqlQuery,
                new
                {
                    minKey = keyMaps.Keys.Min(),
                    maxKey = keyMaps.Keys.Max(),
                    statusName,
                })).ToDictionary(x => x.Key, x => x.Count);


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
    }

    class TimelineCounter
    {
        public string Key { get; set; }
        public int Count { get; set; }
    }
}