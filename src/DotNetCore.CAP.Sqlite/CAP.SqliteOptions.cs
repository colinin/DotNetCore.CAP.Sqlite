﻿// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using DotNetCore.CAP.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;

namespace DotNetCore.CAP
{
    public class SqliteOptions : EFOptions
    {
        /// <summary>
        /// Gets or sets the database's connection string that will be used to store database entities.
        /// </summary>
        public string ConnectionString { get; set; }
    }

    internal class ConfigureSqliteOptions : IConfigureOptions<SqliteOptions>
    {
        private readonly IServiceScopeFactory _serviceScopeFactory;

        public ConfigureSqliteOptions(IServiceScopeFactory serviceScopeFactory)
        {
            _serviceScopeFactory = serviceScopeFactory;
        }

        public void Configure(SqliteOptions options)
        {
            if (options.DbContextType != null)
            {
                if (Helper.IsUsingType<ICapPublisher>(options.DbContextType))
                    throw new InvalidOperationException(
                        "We detected that you are using ICapPublisher in DbContext, please change the configuration to use the storage extension directly to avoid circular references! eg:  x.UseSqlite()");

                using var scope = _serviceScopeFactory.CreateScope();
                var provider = scope.ServiceProvider;
                using var dbContext = (DbContext)provider.GetRequiredService(options.DbContextType);
                var connectionString = dbContext.Database.GetConnectionString();
                if (string.IsNullOrEmpty(connectionString)) throw new ArgumentNullException(connectionString);
                options.ConnectionString = connectionString;
            }
        }
    }
}