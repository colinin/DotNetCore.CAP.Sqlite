https://github.com/dotnetcore/CAP

CAP DataStorage with Sqlite

````C#
// Use

services.AddCap(x =>
            {
                x.UseSqlite(cfg =>
                {
                    cfg.ConnectionString = "Data Source=.\\eventpublisher.db";
                });
                
                //x.UseSqlite("Data Source=.\\eventpublisher.db");
                
                // other...
            });

````
