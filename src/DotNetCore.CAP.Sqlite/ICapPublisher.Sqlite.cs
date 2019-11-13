// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using Dapper;
using DotNetCore.CAP.Abstractions;
using DotNetCore.CAP.Models;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace DotNetCore.CAP.Sqlite
{
    public class SqlitePublisher : CapPublisherBase, ICallbackPublisher
    {
        private readonly SqliteOptions _options;

        public SqlitePublisher(IServiceProvider provider) : base(provider)
        {
            _options = provider.GetService<IOptions<SqliteOptions>>().Value;
        }

        public async Task PublishCallbackAsync(CapPublishedMessage message)
        {
            await PublishAsyncInternal(message);
        }

        protected override async Task ExecuteAsync(CapPublishedMessage message,
            ICapTransaction transaction = null,
            CancellationToken cancel = default(CancellationToken))
        {
            if (transaction == null)
            {
                using (var connection = new SqliteConnection(_options.ConnectionString))
                {
                    await connection.ExecuteAsync(PrepareSql(), message);
                    return;
                }
            }

            var dbTrans = transaction.DbTransaction as IDbTransaction;
            if (dbTrans == null && transaction.DbTransaction is IDbContextTransaction dbContextTrans)
            {
                dbTrans = dbContextTrans.GetDbTransaction();
            }

            var conn = dbTrans?.Connection;
            await conn.ExecuteAsync(PrepareSql(), message, dbTrans);
        }

        #region private methods

        private string PrepareSql()
        {
            return
                $"INSERT INTO `{_options.TableNamePrefix}.published` (`Id`,`Version`,`Name`,`Content`,`Retries`,`Added`,`ExpiresAt`,`StatusName`)" +
                $"VALUES(@Id,'{_options.Version}',@Name,@Content,@Retries,@Added,@ExpiresAt,@StatusName);";
        }

        #endregion private methods
    }
}