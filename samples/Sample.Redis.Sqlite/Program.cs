using Sample.Redis.Sqlite;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddDbContext<AppDbContext>();

builder.Services.AddCap(x =>
{
    x.UseEntityFramework<AppDbContext>();
    x.UseRedis(redis =>
    {
        redis.Configuration = ConfigurationOptions.Parse("localhost");
        redis.OnConsumeError = context =>
        {
            throw new InvalidOperationException("");
        };
    });
    x.UseDashboard();
});

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

// Configure the HTTP request pipeline
app.UseRouting();
app.MapControllers();

app.Run();