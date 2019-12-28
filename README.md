https://github.com/dotnetcore/CAP

CAP的SQLite实现

为了个人Xamarin项目创建，违背了CAP的最终一致性原则，仅供参考

````C#
// 使用方法

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
