using Dapper;
using DotNetCore.CAP.Persistence;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace DotNetCore.CAP.Sqlite;

public class SqliteStorageInitializer : IStorageInitializer
{
    private readonly string _tablePrefix;
    private readonly ILogger _logger;
    private readonly IOptions<SqliteOptions> _options;
    private readonly IOptions<CapOptions> _capOptions;

    public SqliteStorageInitializer(
        ILogger<SqliteStorageInitializer> logger,
        IOptions<SqliteOptions> options,
        IOptions<CapOptions> capOptions)
    {
        _options = options;
        _logger = logger;
        _tablePrefix = _options.Value.TableNamePrefix;
        _capOptions = capOptions;

    }

    public string GetPublishedTableName()
    {
        return $"{_tablePrefix}.Published";
    }

    public string GetReceivedTableName()
    {
        return $"{_tablePrefix}.Received";
    }

    public string GetLockTableName()
    {
        return $"{_tablePrefix}.Locks";
    }

    public async virtual Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return;
        }
        var sql = CreateDbTablesScript();
        await using (var connection = new SqliteConnection(_options.Value.ConnectionString))
        {
            var sqlParam = new
            {
                PubKey = $"publish_retry_{_capOptions.Value.Version}",
                RecKey = $"received_retry_{_capOptions.Value.Version}",
                LastLockTime = DateTime.MinValue,
            };

            await connection.ExecuteAsync(sql, sqlParam);
        }

        _logger.LogDebug("Ensuring all create database tables script are applied.");
    }

    protected virtual string CreateDbTablesScript()
    {
        var batchSql =
            $@"
CREATE TABLE IF NOT EXISTS `{GetReceivedTableName()}` (
  `Id` bigint NOT NULL,
  `Version` varchar(20) DEFAULT NULL COLLATE NOCASE,
  `Name` varchar(400) NOT NULL COLLATE NOCASE,
  `Group` varchar(200) DEFAULT NULL COLLATE NOCASE,
  `Content` longtext,
  `Retries` int(11) DEFAULT NULL,
  `Added` datetime NOT NULL,
  `ExpiresAt` datetime DEFAULT NULL,
  `StatusName` varchar(50) NOT NULL COLLATE NOCASE,
  PRIMARY KEY (`Id`)
); 
CREATE INDEX IF NOT EXISTS `IX_Version_ExpiresAt_StatusName` ON `{GetReceivedTableName()}`(`Version`, `ExpiresAt`, `StatusName`);
CREATE INDEX IF NOT EXISTS `IX_ExpiresAt_StatusName` ON `{GetReceivedTableName()}`(`ExpiresAt`, `StatusName`);

CREATE TABLE IF NOT EXISTS `{GetPublishedTableName()}` (
  `Id` bigint NOT NULL,
  `Version` varchar(20) DEFAULT NULL COLLATE NOCASE,
  `Name` varchar(200) NOT NULL COLLATE NOCASE,
  `Content` longtext,
  `Retries` int(11) DEFAULT NULL,
  `Added` datetime NOT NULL,
  `ExpiresAt` datetime DEFAULT NULL,
  `StatusName` varchar(40) NOT NULL COLLATE NOCASE,
  PRIMARY KEY (`Id`)
);
CREATE INDEX IF NOT EXISTS `IX_Version_ExpiresAt_StatusName` ON `{GetPublishedTableName()}`(`Version`, `ExpiresAt`, `StatusName`);
CREATE INDEX IF NOT EXISTS `IX_ExpiresAt_StatusName` ON `{GetPublishedTableName()}`(`ExpiresAt`, `StatusName`);";

        if (_capOptions.Value.UseStorageLock)
        {
            batchSql += $@"
CREATE TABLE IF NOT EXISTS `{GetLockTableName()}` (
  `Key` varchar(128) NOT NULL COLLATE NOCASE,
  `Instance` varchar(256) DEFAULT NULL COLLATE NOCASE,
  `LastLockTime` datetime DEFAULT NULL,
  PRIMARY KEY (`Key`)
);

INSERT OR IGNORE INTO `{GetLockTableName()}` (`Key`,`Instance`,`LastLockTime`) VALUES(@PubKey, '', @LastLockTime);
INSERT OR IGNORE INTO `{GetLockTableName()}` (`Key`,`Instance`,`LastLockTime`) VALUES(@RecKey, '', @LastLockTime);";
        }
        
        return batchSql;
    }
}
