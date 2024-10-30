// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Dapper;
using DotNetCore.CAP.Internal;
using DotNetCore.CAP.Messages;
using DotNetCore.CAP.Monitoring;
using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Serialization;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace DotNetCore.CAP.Sqlite;

public class SqliteDataStorage : IDataStorage
{
    private readonly IOptions<CapOptions> _capOptions;
    private readonly IOptions<SqliteOptions> _options;
    private readonly IStorageInitializer _initializer;
    private readonly ISerializer _serializer;
    private readonly ISnowflakeId _snowflakeId;

    public SqliteDataStorage(
        IOptions<SqliteOptions> options, 
        IOptions<CapOptions> capOptions,
        IStorageInitializer initializer,
        ISerializer serializer,
        ISnowflakeId snowflakeId
        )
    {
        _options = options;
        _capOptions = capOptions;
        _serializer = serializer;
        _initializer = initializer;
        _snowflakeId = snowflakeId;
    }

    #region 7.1.1

    public async virtual Task<bool> AcquireLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default)
    {
        string sql = $"UPDATE `{_initializer.GetLockTableName()}` SET `Instance` = @Instance,`LastLockTime` = @LastLockTime WHERE `Key` = @Key AND `LastLockTime` < @TTL;";

        var sqlParam = new
        {
            Instance = instance,
            LastLockTime = DateTime.Now,
            Key = key,
            TTL = DateTime.Now.Subtract(ttl),
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        var opResult = await connection.ExecuteAsync(sql, sqlParam);

        return opResult > 0;
    }

    public async virtual Task ReleaseLockAsync(string key, string instance, CancellationToken token = default)
    {
        string sql = $"UPDATE `{_initializer.GetLockTableName()}` SET `Instance` = '',`LastLockTime` = @LastLockTime WHERE `Key` = @Key AND `Instance` = @Instance;";

        var sqlParam = new
        {
            Instance = instance,
            LastLockTime = DateTime.MinValue,
            Key = key,
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.ExecuteAsync(sql, sqlParam);
    }

    public async virtual Task RenewLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default)
    {
        var sql = $"UPDATE `{_initializer.GetLockTableName()}` SET `LastLockTime` = DATETIME(`LastLockTime`, '+{ttl.TotalSeconds} SECONDS') WHERE `Key` = @Key AND `Instance` = @Instance;";

        var sqlParam = new
        {
            Instance = instance,
            Key = key,
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.ExecuteAsync(sql, sqlParam);
    }

    #endregion

    public async Task ChangePublishStateToDelayedAsync(string[] ids)
    {
        var sql = $"UPDATE `{_initializer.GetPublishedTableName()}` SET `StatusName`='{StatusName.Delayed}' WHERE `Id` IN ({string.Join(',', ids)});";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.ExecuteAsync(sql);
    }

    public virtual async Task ChangePublishStateAsync(MediumMessage message, StatusName state, object transaction = null)
    {
        await ChangeMessageStateAsync(_initializer.GetPublishedTableName(), message, state, transaction);
    }

    public virtual async Task ChangeReceiveStateAsync(MediumMessage message, StatusName state)
    {
        await ChangeMessageStateAsync(_initializer.GetReceivedTableName(), message, state);
    }

    public async virtual Task<MediumMessage> StoreMessageAsync(string name, Message content, object dbTransaction = null)
    {
        var sql = $"INSERT INTO `{_initializer.GetPublishedTableName()}` (`Id`,`Version`,`Name`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`)" +
            $"VALUES(@Id,@Version,@Name,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";

        var message = new MediumMessage
        {
            DbId = content.GetId(),
            Origin = content,
            Content = _serializer.Serialize(content),
            Added = DateTime.Now,
            ExpiresAt = null,
            Retries = 0
        };

        var sqlParam = new
        {
            Id = message.DbId,
            Version = _options.Value.Version,
            Name = name,
            message.Content,
            message.Retries,
            message.Added,
            message.ExpiresAt,
            StatusName = nameof(StatusName.Scheduled)
        };

        if (dbTransaction == null)
        {
            await using var connection = new SqliteConnection(_options.Value.ConnectionString);
            await connection.ExecuteAsync(sql, sqlParam);
        }
        else
        {
            var dbTrans = dbTransaction as IDbTransaction;
            if (dbTrans == null && dbTransaction is IDbContextTransaction dbContextTrans)
            {
                dbTrans = dbContextTrans.GetDbTransaction();
            }
                
            var conn = dbTrans?.Connection;
            await conn.ExecuteAsync(sql, sqlParam, dbTrans);
        }

        return message;
    }

    public async virtual Task StoreReceivedExceptionMessageAsync(string name, string group, string content)
    {
        var sqlParam = new
        {
            Id = _snowflakeId.NextId().ToString(),
            Version = _options.Value.Version,
            Group = @group,
            Name = name,
            Content = content,
            Retries = _capOptions.Value.FailedRetryCount,
            Added = DateTime.Now,
            ExpiresAt = DateTime.Now.AddDays(15),
            StatusName = nameof(StatusName.Failed)
        };
        await StoreReceivedMessageAsync(sqlParam);
    }

    public async virtual Task<MediumMessage> StoreReceivedMessageAsync(string name, string group, Message message)
    {
        var mdMessage = new MediumMessage
        {
            DbId = _snowflakeId.NextId().ToString(),
            Origin = message,
            Added = DateTime.Now,
            ExpiresAt = null,
            Retries = 0
        };
        var content = _serializer.Serialize(mdMessage.Origin);
        var sqlParam = new
        {
            Id = mdMessage.DbId,
            Version = _options.Value.Version,
            Group = @group,
            Name = name,
            Content = content,
            mdMessage.Retries,
            mdMessage.Added,
            mdMessage.ExpiresAt,
            StatusName = nameof(StatusName.Scheduled)
        };

        await StoreReceivedMessageAsync(sqlParam);

        return mdMessage;
    }

    public virtual async Task<int> DeleteExpiresAsync(string table, DateTime timeout, int batchCount = 1000, CancellationToken token = default)
    {
        // TODO: Need to enable limit support
        // https://sqlite.org/compile.html#enable_update_delete_limit
        var sql = $"DELETE FROM `{table}` WHERE ExpiresAt < @timeout AND StatusName IN ('{StatusName.Succeeded}','{StatusName.Failed}') ";
        var sqlParam = new { timeout = timeout };
        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        var removedCount = await connection.ExecuteAsync(sql, sqlParam);
        return removedCount;
    }

    public virtual async Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
    {
        return await GetMessagesOfNeedRetryAsync(_initializer.GetPublishedTableName(), lookbackSeconds);
    }

    public virtual async Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry(TimeSpan lookbackSeconds)
    {
        return await GetMessagesOfNeedRetryAsync(_initializer.GetReceivedTableName(), lookbackSeconds);
    }

    public async Task ScheduleMessagesOfDelayedAsync(
        Func<object, IEnumerable<MediumMessage>, Task> scheduleTask,
        CancellationToken token = default)
    {
        var sql =
            $"SELECT `Id`,`Content`,`Retries`,`Added`,`ExpiresAt` FROM `{_initializer.GetPublishedTableName()}` WHERE `Version` = @Version " +
            $"AND ((`ExpiresAt` < @TwoMinutesLater AND `StatusName` = '{StatusName.Delayed}') OR (`ExpiresAt` < @OneMinutesAgo AND `StatusName` = '{StatusName.Queued}'));";

        object sqlParam = new
        {
            Version = _capOptions.Value.Version,
            TwoMinutesLater = DateTime.Now.AddMinutes(2),
            OneMinutesAgo = DateTime.Now.AddMinutes(-1),
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.OpenAsync(token);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, token);

        var reader = await connection.ExecuteReaderAsync(sql, sqlParam, transaction);

        var messages = new List<MediumMessage>();
        while (await reader.ReadAsync(token))
        {
            messages.Add(new MediumMessage
            {
                DbId = reader.GetInt64(0).ToString(),
                Origin = _serializer.Deserialize(reader.GetString(1))!,
                Retries = reader.GetInt32(2),
                Added = reader.GetDateTime(3),
                ExpiresAt = reader.GetDateTime(4)
            });
        }

        await scheduleTask(transaction, messages);

        await transaction.CommitAsync(token);
    }

    IMonitoringApi IDataStorage.GetMonitoringApi()
    {
        return new SqliteMonitoringApi(_options, _initializer, _serializer);
    }

    protected async virtual Task StoreReceivedMessageAsync(object sqlParam)
    {
        var sql = $@"INSERT INTO `{_initializer.GetReceivedTableName()}`(`Id`,`Version`,`Name`,`Group`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`) VALUES(@Id,@Version,@Name,@Group,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.ExecuteAsync(sql, sqlParam);
    }

    protected virtual async Task ChangeMessageStateAsync(string tableName, MediumMessage message, StatusName state, object transaction = null)
    {
        var sql =
           $"UPDATE `{tableName}` SET `Content`= @Content,`Retries` = @Retries,`ExpiresAt` = @ExpiresAt,`StatusName` = @StatusName WHERE `Id` = @Id";
        var sqlParam = new
        {
            Id = message.DbId,
            Retries = message.Retries,
            ExpiresAt = message.ExpiresAt,
            StatusName = state.ToString("G"),
            Content = _serializer.Serialize(message.Origin)
        };

        var dbTransaction = transaction as IDbTransaction;
        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.ExecuteAsync(sql, sqlParam, dbTransaction);
    }

    protected virtual async Task<IEnumerable<MediumMessage>> GetMessagesOfNeedRetryAsync(string tableName, TimeSpan lookbackSeconds)
    {
        var fourMinAgo = DateTime.Now.Subtract(lookbackSeconds);
        //var fourMinAgo = DateTime.Now.AddMinutes(-4).ToString("O");
        var sql = $"SELECT `Id`,`Content`,`Retries`,`Added` FROM `{tableName}` WHERE `Retries` < @Retries AND `Version` = @Version AND `Added` < @Added AND `StatusName` IN ('{StatusName.Failed}','{StatusName.Scheduled}') LIMIT 200;";
        var sqlParam = new
        {
            FailedStatusName = nameof(StatusName.Failed),
            ScheduledStatusName = nameof(StatusName.Scheduled),
            Retries = _capOptions.Value.FailedRetryCount,
            Version = _capOptions.Value.Version,
            Added = fourMinAgo
        };
        var result = new List<MediumMessage>();
        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        using var reader = await connection.ExecuteReaderAsync(sql, sqlParam);
        while (await reader.ReadAsync())
        {
            var mediumMessage = new MediumMessage
            {
                DbId = reader.GetInt64(0).ToString(),
                Origin = _serializer.Deserialize(reader.GetString(1)),
                Retries = reader.GetInt32(2),
                Added = reader.GetDateTime(3)
            };
            result.Add(mediumMessage);
        }
        return result;
    }
}