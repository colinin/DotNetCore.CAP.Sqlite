// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

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

    public async virtual Task<MediumMessage?> GetPublishedMessageAsync(long id)
    {
        return await GetMessageAsync(_published, id);
    }

    public async virtual Task<MediumMessage?> GetReceivedMessageAsync(long id)
    {
        return await GetMessageAsync(_received, id);
    }

    public async virtual Task<StatisticsDto> GetStatisticsAsync()
    {
        var sqlBuilder = new StringBuilder();
        sqlBuilder.AppendLine("PRAGMA READ_UNCOMMITTED = 1;")
            .AppendLine("SELECT")
            .AppendLine("(")
            .AppendFormat("SELECT COUNT(`Id`) FROM `{0}` WHERE `StatusName` = 'Succeeded'", _published)
            .AppendLine(") AS PublishedSucceeded,")
            .AppendLine("(")
            .AppendFormat("SELECT COUNT(`Id`) FROM `{0}` WHERE `StatusName` = 'Succeeded'", _received)
            .AppendLine(") AS ReceivedSucceeded,")
            .AppendLine("(")
            .AppendFormat("SELECT COUNT(`Id`) FROM `{0}` WHERE `StatusName` = 'Failed'", _published)
            .AppendLine(") AS PublishedFailed,")
            .AppendLine("(")
            .AppendFormat("SELECT COUNT(`Id`) FROM `{0}` WHERE `StatusName` = 'Failed'", _received)
            .AppendLine(") AS ReceivedFailed,")
            .AppendLine("(")
            .AppendFormat("SELECT COUNT(`Id`) FROM `{0}` WHERE `StatusName` = 'Delayed'", _published)
            .AppendLine(") AS PublishedDelayed;");

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        var statistics = await connection.ExecuteReaderAsync(sqlBuilder.ToString(), async reader =>
        {
            var statisticsDto = new StatisticsDto();

            while (await reader.ReadAsync())
            {
                statisticsDto.PublishedSucceeded = !reader.IsDBNull(0) ? reader.GetInt32(0) : 0;
                statisticsDto.ReceivedSucceeded = !reader.IsDBNull(1) ? reader.GetInt32(1) : 0;
                statisticsDto.PublishedFailed = !reader.IsDBNull(2) ? reader.GetInt32(2) : 0;
                statisticsDto.ReceivedFailed = !reader.IsDBNull(3) ? reader.GetInt32(3) : 0;
                statisticsDto.PublishedDelayed = !reader.IsDBNull(4) ? reader.GetInt32(4) : 0;
            }

            return statisticsDto;
        });

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

        object[] sqlParams =
        {
            new SqliteParameter("@StatusName", queryDto.StatusName ?? string.Empty),
            new SqliteParameter("@Group", queryDto.Group ?? string.Empty),
            new SqliteParameter("@Name", queryDto.Name ?? string.Empty),
            new SqliteParameter("@Content", $"%{queryDto.Content}%"),
            new SqliteParameter("@Offset", queryDto.CurrentPage * queryDto.PageSize),
            new SqliteParameter("@Limit", queryDto.PageSize)
        };

        var sqlQuery =
            $"SELECT * FROM `{tableName}` WHERE 1=1 {where} ORDER BY `Added` DESC LIMIT @Offset,@Limit";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);

        var count = await connection.ExecuteScalarAsync<int>(
            $"SELECT COUNT(1) FROM `{tableName}` WHERE 1 = 1 {where}",
            new SqliteParameter("@StatusName", queryDto.StatusName ?? string.Empty),
            new SqliteParameter("@Group", queryDto.Group ?? string.Empty),
            new SqliteParameter("@Name", queryDto.Name ?? string.Empty),
            new SqliteParameter("@Content", $"%{queryDto.Content}%"));

        var items = await connection.ExecuteReaderAsync(sqlQuery, async reader =>
        {
            var messages = new List<MessageDto>();

            while (await reader.ReadAsync())
            {
                var index = 0;
                messages.Add(new MessageDto
                {
                    Id = reader.GetInt64(index++).ToString(),
                    Version = reader.GetString(index++),
                    Name = reader.GetString(index++),
                    Group = queryDto.MessageType == MessageType.Subscribe ? reader.GetString(index++) : default,
                    Content = reader.GetString(index++),
                    Retries = reader.GetInt32(index++),
                    Added = reader.GetDateTime(index++),
                    ExpiresAt = reader.IsDBNull(index++) ? null : reader.GetDateTime(index - 1),
                    StatusName = reader.GetString(index)
                });
            }

            return messages;
        }, sqlParams: sqlParams);

        return new PagedQueryResult<MessageDto>
        {
            Items = items,
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

    private async Task<MediumMessage?> GetMessageAsync(string tableName, long id)
    {
        var sql = $@"SELECT `Id` as DbId, `Content`,`Added`,`ExpiresAt`,`Retries` FROM `{tableName}` WHERE `Id`=@Id;";
        var sqlParam = new { Id = id }; 
        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        var mediumMessage = await connection.ExecuteReaderAsync(sql, async reader =>
        {
            MediumMessage? message = null;

            while (await reader.ReadAsync())
            {
                message = new MediumMessage
                {
                    DbId = reader.GetInt64(0).ToString(),
                    Origin = _serializer.Deserialize(reader.GetString(1))!,
                    Content = reader.GetString(1),
                    Added = reader.GetDateTime(2),
                    ExpiresAt = !reader.IsDBNull(3) ? reader.GetDateTime(3) : null,
                    Retries = reader.GetInt32(4)
                };
            }

            return message;
        }, sqlParams: new SqliteParameter("@Id", id));

        return mediumMessage;
    }

    private async ValueTask<int> GetNumberOfMessage(string tableName, string statusName)
    {
        var sqlQuery = $"SELECT COUNT(`Id`) FROM `{tableName}` WHERE `StatusName` = @State";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);

        var count = await connection.ExecuteScalarAsync<int>(sqlQuery,
            new SqliteParameter("@State", statusName));
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
            WHERE `StatusName` = @StatusName
            GROUP BY STRFTIME('%Y-%m-%d-%H', `Added`)
        ) AS aggr WHERE aggr.`Key` >= @MinKey AND aggr.`Key` <= @MaxKey;;";

        object[] sqlParams =
        {
            new SqliteParameter("@StatusName", statusName),
            new SqliteParameter("@MinKey", keyMaps.Keys.Min()),
            new SqliteParameter("@MaxKey", keyMaps.Keys.Max())
        };

        Dictionary<string, int> valuesMap;
        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        {
            valuesMap = await connection.ExecuteReaderAsync(sqlQuery, async reader =>
            {
                var dictionary = new Dictionary<string, int>();

                while (await reader.ReadAsync())
                {
                    dictionary.Add(reader.GetString(0), reader.GetInt32(1));
                }

                return dictionary;
            }, sqlParams: sqlParams);
        }

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