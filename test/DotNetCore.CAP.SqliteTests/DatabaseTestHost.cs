using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using System;
using System.IO;

namespace DotNetCore.CAP.Sqlite.Test
{
    public abstract class DatabaseTestHost : IDisposable
    {
        protected ILogger<SqliteStorage> Logger;
        protected IOptions<CapOptions> CapOptions;
        protected IOptions<SqliteOptions> SqliteOptions;

        protected DatabaseTestHost()
        {
            Logger = new Mock<ILogger<SqliteStorage>>().Object;

            var capOptions = new Mock<IOptions<CapOptions>>();
            capOptions.Setup(x => x.Value).Returns(new CapOptions());
            CapOptions = capOptions.Object;

            var options = new Mock<IOptions<SqliteOptions>>();
            options.Setup(x => x.Value).Returns(new SqliteOptions { ConnectionString = ConnectionUtil.GetConnectionString() });
            SqliteOptions = options.Object;
            InitializeDatabase();
        }

        public void Dispose()
        {
            DeleteAllData();
        }

        private void InitializeDatabase()
        {
            var sqliteConn = ConnectionUtil.GetConnectionString();
            var databaseName = ConnectionUtil.GetDatabaseName();
            if (!File.Exists(databaseName))
            {
                using (var connection = ConnectionUtil.CreateConnection(sqliteConn))
                {
                    connection.Open();
                    connection.Close();
                }
            }
            new SqliteStorage(Logger, SqliteOptions, CapOptions).InitializeAsync().GetAwaiter().GetResult();
        }


        private void DeleteAllData()
        {
            var databaseName = ConnectionUtil.GetDatabaseName();
            if (File.Exists(databaseName))
            {
                File.Delete(databaseName);
            }
        }
    }
}
