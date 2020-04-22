// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using DotNetCore.CAP.Persistence;
using DotNetCore.CAP.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using System;

// ReSharper disable once CheckNamespace
namespace DotNetCore.CAP
{
    internal class SqliteCapOptionsExtension : ICapOptionsExtension
    {
        private readonly Action<SqliteOptions> _configure;

        public SqliteCapOptionsExtension(Action<SqliteOptions> configure)
        {
            _configure = configure;
        }

        public void AddServices(IServiceCollection services)
        {
            services.AddSingleton<CapStorageMarkerService>();

            services.AddSingleton<IDataStorage, SqliteDataStorage>();
            services.AddSingleton<IStorageInitializer, SqliteStorageInitializer>();
            services.AddSingleton<ICapTransaction, SqliteCapTransaction>();


            //Add SqliteOptions
            services.Configure(_configure);
            services.AddSingleton<IConfigureOptions<SqliteOptions>, ConfigureSqliteOptions>();
        } 
    }
}