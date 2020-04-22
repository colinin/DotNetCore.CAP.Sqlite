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

namespace DotNetCore.CAP.Sqlite
{
    public class SqliteDataStorage : IDataStorage
    {
        private readonly IOptions<CapOptions> _capOptions;
        private readonly IOptions<SqliteOptions> _options;
        private readonly IStorageInitializer _initializer;

        public SqliteDataStorage(
            IOptions<SqliteOptions> options, 
            IOptions<CapOptions> capOptions,
            IStorageInitializer initializer)
        {
            _options = options;
            _capOptions = capOptions;
            _initializer = initializer;
        }

        public async Task ChangePublishStateAsync(MediumMessage message, StatusName state)
        {
            var sql =
                $"UPDATE `{_initializer.GetPublishedTableName()}` SET `Retries` = @Retries,`ExpiresAt` = @ExpiresAt,`StatusName` = @StatusName WHERE `Id` = @Id";
            var sqlParam = new
            {
                Id = message.DbId,
                Retries = message.Retries,
                ExpiresAt = message.ExpiresAt,
                StatusName = state.ToString("G")
            };

            using (var connection = new SqliteConnection(_options.Value.ConnectionString))
            {
                await connection.ExecuteAsync(sql, sqlParam);
            };
        }

        public async Task ChangeReceiveStateAsync(MediumMessage message, StatusName state)
        {
            var sql =
                $"UPDATE `{_initializer.GetReceivedTableName()}` SET `Retries` = @Retries,`ExpiresAt` = @ExpiresAt,`StatusName` = @StatusName WHERE `Id` = @Id";
            var sqlParam = new
            {
                Id = message.DbId,
                Retries = message.Retries,
                ExpiresAt = message.ExpiresAt,
                StatusName = state.ToString("G")
            };

            using (var connection = new SqliteConnection(_options.Value.ConnectionString))
            {
                await connection.ExecuteAsync(sql, sqlParam);
            };
        }

        public MediumMessage StoreMessage(string name, Message content, object dbTransaction = null)
        {
            var sql = $"INSERT INTO `{_initializer.GetPublishedTableName()}` (`Id`,`Version`,`Name`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`)" +
                $"VALUES(@Id,@Version,@Name,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";

            var message = new MediumMessage
            {
                DbId = content.GetId(),
                Origin = content,
                Content = StringSerializer.Serialize(content),
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
                using (var connection = new SqliteConnection(_options.Value.ConnectionString))
                {
                    connection.Execute(sql, sqlParam);
                }
            }
            else
            {
                var dbTrans = dbTransaction as IDbTransaction;
                if (dbTrans == null && dbTransaction is IDbContextTransaction dbContextTrans)
                {
                    dbTrans = dbContextTrans.GetDbTransaction();
                }
                    
                var conn = dbTrans?.Connection;
                conn.Execute(sql, sqlParam, dbTrans);
            }

            return message;
        }

        public void StoreReceivedExceptionMessage(string name, string group, string content)
        {
            var sql = $@"INSERT INTO `{_initializer.GetReceivedTableName()}`(`Id`,`Version`,`Name`,`Group`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`) VALUES(@Id,@Version,@Name,@Group,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";
            var sqlParam = new
            {
                Id = SnowflakeId.Default().NextId().ToString(),
                Version = _options.Value.Version,
                Group = @group,
                Name = name,
                Content = content,
                Retries = _capOptions.Value.FailedRetryCount,
                Added = DateTime.Now,
                ExpiresAt = DateTime.Now.AddDays(15),
                StatusName = nameof(StatusName.Failed)
            };
            using (var connection = new SqliteConnection(_options.Value.ConnectionString))
            {
                connection.Execute(sql, sqlParam);
            }
        }

        public MediumMessage StoreReceivedMessage(string name, string group, Message message)
        {
            var sql = $@"INSERT INTO `{_initializer.GetReceivedTableName()}`(`Id`,`Version`,`Name`,`Group`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`) VALUES(@Id,@Version,@Name,@Group,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";
            var mdMessage = new MediumMessage
            {
                DbId = SnowflakeId.Default().NextId().ToString(),
                Origin = message,
                Added = DateTime.Now,
                ExpiresAt = null,
                Retries = 0
            };
            var content = StringSerializer.Serialize(mdMessage.Origin);
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
            using (var connection = new SqliteConnection(_options.Value.ConnectionString))
            {
                connection.Execute(sql, sqlParam);
            }
                
            return mdMessage;
        }

        public async Task<int> DeleteExpiresAsync(string table, DateTime timeout, int batchCount = 1000, CancellationToken token = default)
        {
            // TODO: Need to enable limit support
            // https://sqlite.org/compile.html#enable_update_delete_limit
            var sql = $"DELETE FROM `{table}` WHERE ExpiresAt < @timeout";
            var sqlParam = new { timeout = timeout };
            using (var connection = new SqliteConnection(_options.Value.ConnectionString))
            {
                var removedCount = await connection.ExecuteAsync(sql, sqlParam);
                return removedCount;
            }
        }

        public async Task<IEnumerable<MediumMessage>> GetPublishedMessagesOfNeedRetry()
        {
            var fourMinAgo = DateTime.Now.AddMinutes(-4).ToString("O");
            var sql = $"SELECT * FROM `{_initializer.GetPublishedTableName()}` WHERE `Retries` < @Retries AND `Version` = @Version AND `Added` < @Added AND (`StatusName` = @FailedStatusName OR `StatusName` = @ScheduledStatusName) LIMIT 200;";
            var sqlParam = new
            {
                FailedStatusName = nameof(StatusName.Failed),
                ScheduledStatusName = nameof(StatusName.Scheduled),
                Retries = _capOptions.Value.FailedRetryCount,
                Version = _capOptions.Value.Version,
                Added = fourMinAgo
            };
            var result = new List<MediumMessage>();
            using (var connection = new SqliteConnection(_options.Value.ConnectionString))
            {
                var reader = await connection.ExecuteReaderAsync(sql, sqlParam);
                while (reader.Read())
                {
                    result.Add(new MediumMessage
                    {
                        DbId = reader.GetInt64(0).ToString(),
                        Origin = StringSerializer.DeSerialize(reader.GetString(3)),
                        Retries = reader.GetInt32(4),
                        Added = reader.GetDateTime(5)
                    });
                }
                return result;
            }
        }

        public async Task<IEnumerable<MediumMessage>> GetReceivedMessagesOfNeedRetry()
        {
            var fourMinsAgo = DateTime.Now.AddMinutes(-4).ToString("O");
            var sql = $"SELECT * FROM `{_initializer.GetReceivedTableName()}` WHERE `Retries` < @Retries AND `Version` = @Version AND `Added` < @Added AND (`StatusName` = @FailedStatusName OR `StatusName` = @ScheduledStatusName) LIMIT 200;";
            var sqlParam = new
            {
                FailedStatusName = nameof(StatusName.Failed),
                ScheduledStatusName = nameof(StatusName.Scheduled),
                Retries = _capOptions.Value.FailedRetryCount,
                Version = _capOptions.Value.Version,
                Added = fourMinsAgo
            };
            var result = new List<MediumMessage>();
            using (var connection = new SqliteConnection(_options.Value.ConnectionString))
            {
                var reader = await connection.ExecuteReaderAsync(sql, sqlParam);
                while (reader.Read())
                {
                    result.Add(new MediumMessage
                    {
                        DbId = reader.GetInt64(0).ToString(),
                        Origin = StringSerializer.DeSerialize(reader.GetString(3)),
                        Retries = reader.GetInt32(4),
                        Added = reader.GetDateTime(5)
                    });
                }
                return result;
            }
        }

        IMonitoringApi IDataStorage.GetMonitoringApi()
        {
            return new SqliteMonitoringApi(_options, _initializer);
        }
    }
}