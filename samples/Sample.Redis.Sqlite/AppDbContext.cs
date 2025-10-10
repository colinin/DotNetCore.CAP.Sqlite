using Microsoft.EntityFrameworkCore;

namespace Sample.Redis.Sqlite;

public class Person
{
    public int Id { get; set; }

    public string Name { get; set; } = default!;

    public int Age { get; set; }

    public override string ToString()
    {
        return $"Name:{Name}, Age:{Age}";
    }
}

public class AppDbContext : DbContext
{
    public const string ConnectionString = "Data Source=./Sample.Redis.Sqlite.db";
    public DbSet<Person> Persons { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseSqlite(ConnectionString);
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Person>(e =>
        {
            e.ToTable("persons");

            e.Property(p => p.Name)
             .HasMaxLength(100)
             .IsRequired(true);

            e.HasKey(p => p.Id);
        });
    }
}
