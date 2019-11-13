// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Dapper;
using DotNetCore.CAP.Processor;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DotNetCore.CAP.Sqlite
{
    internal class SqliteCollectProcessor : ICollectProcessor
    {
        private const int MaxBatch = 1000;
        private readonly TimeSpan _delay = TimeSpan.FromSeconds(1);
        private readonly ILogger _logger;
        private readonly SqliteOptions _options;
        private readonly TimeSpan _waitingInterval = TimeSpan.FromMinutes(5);

        public SqliteCollectProcessor(ILogger<SqliteCollectProcessor> logger, IOptions<SqliteOptions> mysqlOptions)
        {
            _logger = logger;
            _options = mysqlOptions.Value;
        }

        public async Task ProcessAsync(ProcessingContext context)
        {
            var tables = new[]
            {
                $"{_options.TableNamePrefix}.published",
                $"{_options.TableNamePrefix}.received"
            };
            
            foreach (var table in tables)
            {
                _logger.LogDebug($"Collecting expired data from table [{table}].");

                int removedCount;
                do
                {
                    using (var connection = new SqliteConnection(_options.ConnectionString))
                    {
                        // TODO: Need to enable limit support
                        // https://sqlite.org/compile.html#enable_update_delete_limit
                        removedCount = await connection.ExecuteAsync(
                                $@"DELETE FROM `{table}` WHERE `ExpiresAt` < @now",
                            new { now = DateTime.Now });
                    }

                    if (removedCount != 0)
                    {
                        await context.WaitAsync(_delay);
                        context.ThrowIfStopping();
                    }
                } while (removedCount != 0);
            }

            await context.WaitAsync(_waitingInterval);
        }
    }
}