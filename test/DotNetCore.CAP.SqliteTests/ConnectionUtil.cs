using Microsoft.Data.Sqlite;
using System;

namespace DotNetCore.CAP.Sqlite.Test
{
    public static class ConnectionUtil
    {
        //private const string DatabaseVariable = "Cap_Sqlite_DatabaseName";
        //private const string ConnectionStringTemplateVariable = "Cap_Sqlite_ConnectionStringTemplate";

        //private const string DefaultDatabaseName = @".\DotNetCore.CAP.Sqlite.Test.db";

        //private const string DefaultConnectionStringTemplate =
        //    @"Data Source=.\DotNetCore.CAP.Sqlite.Test.db";

        //public static string GetDatabaseName()
        //{
        //    return Environment.GetEnvironmentVariable(DatabaseVariable) 
        //            ?? DefaultDatabaseName;
        //}

        public static string GetConnectionString(string databaseName)
        {
            return $"Data Source={databaseName}";
        }

        public static SqliteConnection CreateConnection(string connectionString)
        {
            var connection = new SqliteConnection(connectionString);
            connection.Open();
            return connection;
        }
    }
}