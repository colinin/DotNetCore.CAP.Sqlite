using DotNetCore.CAP.Persistence;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.IO;
using System.Threading;

namespace DotNetCore.CAP.Sqlite.Test
{
    public abstract class DatabaseTestHost : IDisposable
    {
        protected virtual string DataBaseName => @".\DotNetCore.CAP.Sqlite.Test.db";
        private readonly IServiceCollection _services;
        private readonly IServiceProvider _serviceProvider;
        protected ILogger<SqliteDataStorage> Logger;
        protected IOptions<CapOptions> CapOptions;
        protected IOptions<SqliteOptions> SqliteOptions;

        protected DatabaseTestHost()
        {
            _services = new ServiceCollection();
            _services.AddOptions();
            _services.AddLogging();
            _services.AddCap(options =>
            {
                options.UseSqlite(ConnectionUtil.GetConnectionString(DataBaseName));
            });

            _serviceProvider = _services.BuildServiceProvider();

            Logger = new Mock<ILogger<SqliteDataStorage>>().Object;

            CapOptions = GetRequiredService<IOptions<CapOptions>>();
            SqliteOptions = GetRequiredService<IOptions<SqliteOptions>>();

            InitializeDatabase();
        }

        protected T GetService<T>()
        {
            return _serviceProvider.GetService<T>();
        }

        protected T GetRequiredService<T>()
        {
            return _serviceProvider.GetRequiredService<T>();
        }

        public void Dispose()
        {
            DeleteAllData();
        }

        private void InitializeDatabase()
        {
            var sqliteConn = ConnectionUtil.GetConnectionString(DataBaseName);
            if (!File.Exists(DataBaseName))
            {
                using (var connection = ConnectionUtil.CreateConnection(sqliteConn))
                {
                    connection.Open();
                    var storage = _serviceProvider.GetService<IStorageInitializer>();
                    var token = new CancellationTokenSource().Token;
                    storage.InitializeAsync(token).GetAwaiter().GetResult();
                    connection.Close();
                }
            }
        }


        private void DeleteAllData()
        {
            if (File.Exists(DataBaseName))
            {
                File.Delete(DataBaseName);
            }
        }
    }
}
