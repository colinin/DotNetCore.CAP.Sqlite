﻿// Copyright (c) .NET Core Community. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using DotNetCore.CAP;
using Microsoft.EntityFrameworkCore;

// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.DependencyInjection;

public static class CapOptionsExtensions
{
    public static CapOptions UseSqlite(this CapOptions options, string connectionString)
    {
        return options.UseSqlite(opt => { opt.ConnectionString = connectionString; });
    }

    public static CapOptions UseSqlite(this CapOptions options, Action<SqliteOptions> configure)
    {
        if (configure == null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        configure += x => x.Version = options.Version;

        options.RegisterExtension(new SqliteCapOptionsExtension(configure));

        return options;
    }

    public static CapOptions UseEntityFramework<TContext>(this CapOptions options)
        where TContext : DbContext
    {
        return options.UseEntityFramework<TContext>(opt => { });
    }

    public static CapOptions UseEntityFramework<TContext>(this CapOptions options, Action<EFOptions> configure)
        where TContext : DbContext
    {
        if (configure == null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        options.RegisterExtension(new SqliteCapOptionsExtension(x =>
        {
            configure(x);
            x.DbContextType = typeof(TContext);
            x.Version = options.Version;
        }));

        return options;
    }
}