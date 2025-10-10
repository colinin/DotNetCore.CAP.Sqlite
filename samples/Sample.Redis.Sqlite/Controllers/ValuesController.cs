using Dapper;
using DotNetCore.CAP;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.Sqlite;
using System.Data;

namespace Sample.Redis.Sqlite.Controllers;

[Route("api/[controller]")]
public class ValuesController : Controller
{
    private readonly ICapPublisher _capBus;

    public ValuesController(ICapPublisher capPublisher)
    {
        _capBus = capPublisher;
    }

    [Route("~/control/start")]
    [HttpPost]
    public async Task<IActionResult> Start([FromServices] IBootstrapper bootstrapper)
    {
        await bootstrapper.BootstrapAsync();
        return Ok();
    }

    [Route("~/control/stop")]
    [HttpPost]
    public async Task<IActionResult> Stop([FromServices] IBootstrapper bootstrapper)
    {
        await bootstrapper.DisposeAsync();
        return Ok();
    }

    [Route("~/without/transaction")]
    [HttpPost]
    public async Task<IActionResult> WithoutTransactionAsync()
    {
        await _capBus.PublishAsync("sample.redis.sqlite", DateTime.Now, cancellationToken: HttpContext.RequestAborted);

        return Ok();
    }

    [Route("~/delay/{delaySeconds:int}")]
    [HttpPost]
    public async Task<IActionResult> Delay(int delaySeconds)
    {
        await _capBus.PublishDelayAsync(TimeSpan.FromSeconds(delaySeconds), "sample.redis.test", $"publish time:{DateTime.Now}, delay seconds:{delaySeconds}");

        return Ok();
    }

    [Route("~/adonet/transaction")]
    [HttpPost]
    public async Task<IActionResult> AdonetWithTransaction()
    {
        using (var connection = new SqliteConnection(AppDbContext.ConnectionString))
        {
            using var transaction = await connection.BeginTransactionAsync(_capBus, true);
            await connection.ExecuteAsync($"insert into persons(name,age) values('{Guid.NewGuid()}', 1)", transaction: transaction.DbTransaction as IDbTransaction);
            await _capBus.PublishAsync("sample.redis.sqlite", DateTime.Now);
        }

        return Ok();
    }

    [Route("~/ef/transaction")]
    [HttpPost]
    public async Task<IActionResult> EntityFrameworkWithTransaction([FromServices] AppDbContext dbContext)
    {
        using (var trans = await dbContext.Database.BeginTransactionAsync(_capBus, autoCommit: false))
        {
            await dbContext.Persons.AddAsync(new Person() { Name = "ef.transaction" });
            await _capBus.PublishAsync("sample.redis.sqlite", DateTime.Now);
            await dbContext.SaveChangesAsync();
            await trans.CommitAsync();
        }
        return Ok();
    }

    [NonAction]
    [CapSubscribe("sample.redis.sqlite")]
    public void Subscriber(DateTime time)
    {
        Console.WriteLine("Publishing time:" + time);
    }

    [NonAction]
    [CapSubscribe("sample.redis.test")]
    public void Subscriber2(string message)
    {
        Console.WriteLine("Publishing message:" + message);
    }
}
