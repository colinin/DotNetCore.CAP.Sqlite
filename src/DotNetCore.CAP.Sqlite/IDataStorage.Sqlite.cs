// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

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
using System.Data.Common;
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

        object[] sqlParams =
        {
            new SqliteParameter("@Instance", instance),
            new SqliteParameter("@LastLockTime", DateTime.Now),
            new SqliteParameter("@Key", key),
            new SqliteParameter("@TTL", DateTime.Now.Subtract(ttl))
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        var opResult = await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams);

        return opResult > 0;
    }

    public async virtual Task ReleaseLockAsync(string key, string instance, CancellationToken token = default)
    {
        string sql = $"UPDATE `{_initializer.GetLockTableName()}` SET `Instance` = '',`LastLockTime` = @LastLockTime WHERE `Key` = @Key AND `Instance` = @Instance;";

        object[] sqlParams =
        {
            new SqliteParameter("@Instance", instance),
            new SqliteParameter("@LastLockTime", DateTime.MinValue),
            new SqliteParameter("@Key", key)
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams);
    }

    public async virtual Task RenewLockAsync(string key, TimeSpan ttl, string instance, CancellationToken token = default)
    {
        var sql = $"UPDATE `{_initializer.GetLockTableName()}` SET `LastLockTime` = DATETIME(`LastLockTime`, '+{ttl.TotalSeconds} SECONDS') WHERE `Key` = @Key AND `Instance` = @Instance;";

        object[] sqlParams =
        {
            new SqliteParameter("@Instance", instance),
            new SqliteParameter("@Key", key)
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams);
    }

    #endregion

    public async Task ChangePublishStateToDelayedAsync(string[] ids)
    {
        var sql = $"UPDATE `{_initializer.GetPublishedTableName()}` SET `StatusName`='{StatusName.Delayed}' WHERE `Id` IN ({string.Join(',', ids)});";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.ExecuteNonQueryAsync(sql);
    }

    public virtual async Task ChangePublishStateAsync(MediumMessage message, StatusName state, object? transaction = null)
    {
        await ChangeMessageStateAsync(_initializer.GetPublishedTableName(), message, state, transaction);
    }

    public virtual async Task ChangeReceiveStateAsync(MediumMessage message, StatusName state)
    {
        await ChangeMessageStateAsync(_initializer.GetReceivedTableName(), message, state);
    }

    public async virtual Task<MediumMessage> StoreMessageAsync(string name, Message content, object? dbTransaction = null)
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

        object[] sqlParams =
        {
            new SqliteParameter("@Id", message.DbId),
            new SqliteParameter("@Version", _options.Value.Version),
            new SqliteParameter("@Name", name),
            new SqliteParameter("@Content", message.Content),
            new SqliteParameter("@Retries", message.Retries),
            new SqliteParameter("@Added", message.Added),
            new SqliteParameter("@ExpiresAt", message.ExpiresAt.HasValue ? message.ExpiresAt.Value : DBNull.Value),
            new SqliteParameter("@StatusName", nameof(StatusName.Scheduled))
        };

        if (dbTransaction == null)
        {
            await using var connection = new SqliteConnection(_options.Value.ConnectionString);
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams);
        }
        else
        {
            var dbTrans = dbTransaction as DbTransaction;
            if (dbTrans == null && dbTransaction is IDbContextTransaction dbContextTrans)
            {
                dbTrans = dbContextTrans.GetDbTransaction();
            }
                
            var conn = dbTrans!.Connection!;
            await conn.ExecuteNonQueryAsync(sql, dbTrans, sqlParams);
        }

        return message;
    }

    public async virtual Task StoreReceivedExceptionMessageAsync(string name, string group, string content)
    {
        object[] sqlParams =
        {
            new SqliteParameter("@Id", _snowflakeId.NextId().ToString()),
            new SqliteParameter("@Version", _options.Value.Version),
            new SqliteParameter("@Group", group),
            new SqliteParameter("@Name", name),
            new SqliteParameter("@Content", content),
            new SqliteParameter("@Retries", _capOptions.Value.FailedRetryCount),
            new SqliteParameter("@Added", DateTime.Now),
            new SqliteParameter("@ExpiresAt", DateTime.Now.AddSeconds(_capOptions.Value.FailedMessageExpiredAfter)),
            new SqliteParameter("@StatusName", nameof(StatusName.Failed))
        };

        await StoreReceivedMessageAsync(sqlParams);
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

        object[] sqlParams =
        {
            new SqliteParameter("@Id", mdMessage.DbId),
            new SqliteParameter("@Version", _options.Value.Version),
            new SqliteParameter("@Group", group),
            new SqliteParameter("@Name", name),
            new SqliteParameter("@Content", _serializer.Serialize(mdMessage.Origin)),
            new SqliteParameter("@Retries", mdMessage.Retries),
            new SqliteParameter("@Added", mdMessage.Added),
            new SqliteParameter("@ExpiresAt", mdMessage.ExpiresAt.HasValue ? mdMessage.ExpiresAt.Value : DBNull.Value),
            new SqliteParameter("@StatusName", nameof(StatusName.Scheduled))
        };

        await StoreReceivedMessageAsync(sqlParams);

        return mdMessage;
    }

    public virtual async Task<int> DeleteExpiresAsync(string table, DateTime timeout, int batchCount = 1000, CancellationToken token = default)
    {
        // TODO: Need to enable limit support
        // https://sqlite.org/compile.html#enable_update_delete_limit
        var sql = $"DELETE FROM `{table}` WHERE ExpiresAt < @timeout AND StatusName IN ('{StatusName.Succeeded}','{StatusName.Failed}') ";

        object[] sqlParams =
        {
            new SqliteParameter("@timeout", timeout),
            new SqliteParameter("@batchCount", batchCount),
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        var removedCount = await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams);
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

    public async Task<int> DeleteReceivedMessageAsync(long id)
    {
        var sql = $"DELETE FROM `{_initializer.GetReceivedTableName()}` WHERE Id={id};";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        var result = await connection.ExecuteNonQueryAsync(sql);
        return result;
    }

    public async Task<int> DeletePublishedMessageAsync(long id)
    {
        var sql = $"DELETE FROM `{_initializer.GetPublishedTableName()}` WHERE Id={id};";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        var result = await connection.ExecuteNonQueryAsync(sql);
        return result;
    }

    public async Task ScheduleMessagesOfDelayedAsync(
        Func<object, IEnumerable<MediumMessage>, Task> scheduleTask,
        CancellationToken token = default)
    {
        var sql =
            $"SELECT `Id`,`Content`,`Retries`,`Added`,`ExpiresAt` FROM `{_initializer.GetPublishedTableName()}` WHERE `Version` = @Version " +
            $"AND ((`ExpiresAt` < @TwoMinutesLater AND `StatusName` = '{StatusName.Delayed}') OR (`ExpiresAt` < @OneMinutesAgo AND `StatusName` = '{StatusName.Queued}')) LIMIT @BatchSize;";

        object[] sqlParams =
        {
            new SqliteParameter("@Version", _capOptions.Value.Version),
            new SqliteParameter("@TwoMinutesLater", DateTime.Now.AddMinutes(2)),
            new SqliteParameter("@OneMinutesAgo", DateTime.Now.AddMinutes(-1)),
            new SqliteParameter("@BatchSize", _capOptions.Value.SchedulerBatchSize)
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.OpenAsync(token);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, token);

        var messageList = await connection.ExecuteReaderAsync(sql, async reader =>
        {
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

            return messages;
        }, transaction, sqlParams);

        await scheduleTask(transaction, messageList);

        await transaction.CommitAsync(token);
    }

    IMonitoringApi IDataStorage.GetMonitoringApi()
    {
        return new SqliteMonitoringApi(_options, _initializer, _serializer);
    }

    protected async virtual Task StoreReceivedMessageAsync(object[] sqlParams)
    {
        var sql = $@"INSERT INTO `{_initializer.GetReceivedTableName()}`(`Id`,`Version`,`Name`,`Group`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`) 
                     VALUES(@Id,@Version,@Name,@Group,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams);
    }

    protected virtual async Task ChangeMessageStateAsync(string tableName, MediumMessage message, StatusName state, object? transaction = null)
    {
        var sql =
           $"UPDATE `{tableName}` SET `Content`= @Content,`Retries` = @Retries,`ExpiresAt` = @ExpiresAt,`StatusName` = @StatusName WHERE `Id` = @Id";

        object[] sqlParams =
        {
            new SqliteParameter("@Id", message.DbId),
            new SqliteParameter("@Content", _serializer.Serialize(message.Origin)),
            new SqliteParameter("@Retries", message.Retries),
            new SqliteParameter("@ExpiresAt", message.ExpiresAt.HasValue ? message.ExpiresAt.Value : DBNull.Value),
            new SqliteParameter("@StatusName", state.ToString("G"))
        };

        if (transaction is DbTransaction dbTransaction)
        {
            var connection = (SqliteConnection)dbTransaction.Connection!;
            await connection.ExecuteNonQueryAsync(sql, dbTransaction, sqlParams);
        }
        else
        {
            await using var connection = new SqliteConnection(_options.Value.ConnectionString);
            await connection.ExecuteNonQueryAsync(sql, sqlParams: sqlParams);
        }
    }

    protected virtual async Task<IEnumerable<MediumMessage>> GetMessagesOfNeedRetryAsync(string tableName, TimeSpan lookbackSeconds)
    {
        var fourMinAgo = DateTime.Now.Subtract(lookbackSeconds);
        var sql = $"SELECT `Id`,`Content`,`Retries`,`Added` FROM `{tableName}` WHERE `Retries` < @Retries " +
                  $"AND `Version` = @Version AND `Added` < @Added AND `StatusName` IN ('{StatusName.Failed}','{StatusName.Scheduled}') LIMIT 200;";

        object[] sqlParams =
        {
            new SqliteParameter("@Retries", _capOptions.Value.FailedRetryCount),
            new SqliteParameter("@Version", _capOptions.Value.Version),
            new SqliteParameter("@Added", fourMinAgo)
        };

        await using var connection = new SqliteConnection(_options.Value.ConnectionString);
        await connection.OpenAsync();
        var result = await connection.ExecuteReaderAsync(sql, async reader =>
        {
            var messages = new List<MediumMessage>();
            while (await reader.ReadAsync())
            {
                messages.Add(new MediumMessage
                {
                    DbId = reader.GetInt64(0).ToString(),
                    Origin = _serializer.Deserialize(reader.GetString(1))!,
                    Retries = reader.GetInt32(2),
                    Added = reader.GetDateTime(3)
                });
            }

            return messages;
        }, sqlParams: sqlParams);

        return result;
    }
}