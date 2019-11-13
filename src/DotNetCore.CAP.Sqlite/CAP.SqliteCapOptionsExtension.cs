// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using DotNetCore.CAP.Sqlite;
using DotNetCore.CAP.Processor;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

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
            services.AddSingleton<IStorage, SqliteStorage>();
            services.AddSingleton<IStorageConnection, SqliteStorageConnection>();
            services.AddSingleton<ICapPublisher, SqlitePublisher>();
            services.AddSingleton<ICallbackPublisher>(provider => (SqlitePublisher)provider.GetService<ICapPublisher>());
            services.AddSingleton<ICollectProcessor, SqliteCollectProcessor>();

            services.AddTransient<CapTransactionBase, SqliteCapTransaction>();

            //Add SqliteOptions
            services.Configure(_configure);
            services.AddSingleton<IConfigureOptions<SqliteOptions>, ConfigureSqliteOptions>();
        } 
    }
}