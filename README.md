https://github.com/dotnetcore/CAP

CAP DataStorage with Sqlite

````C#
// Use

services.AddCap(x =>
            {
                x.UseSqlite(cfg =>
                {
                    cfg.ConnectionString = "Data Source=./cap-event.db";
                });
                
                //x.UseSqlite("Data Source=./cap-event.db");
                
                // other...
            });

````
