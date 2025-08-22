using Etl.netDemo;
using Etl.netDemo.Jobs;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Paillave.Etl.Core;
using Paillave.Etl.SqlServer;
using Serilog;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Globalization;
using Tps.Dms.Shared.Utilities.Abstractions.Contexts;
using Tps.Dms.Shared.Utilities.Api.Models;
using Tps.Dms.Shared.Utilities.Api.ServiceExtensions;
using Tps.Dms.Shared.Utilities.Application.Contexts;

//var builder = Host.CreateApplicationBuilder(args);

//builder.Services.AddHttpContextAccessor();
//builder.Services.AddScoped<IApplicationContext, ApplicationContext>();

//builder.Services.AddSerilog(builder.Configuration);

//// Add Multi-Tenancy support
//builder.Services.AddMultiTenancy(builder.Configuration, builder.Environment, options =>
//    options.UseSqlServer(builder.Configuration.GetConnectionString("TenantDb")));


//// Add quartz
//builder.Services.AddQuartz(builder.Configuration);

//// Register job classes
//builder.Services.AddScoped<SalesAggregationJob>();

//var openTelemetryOptions = builder.Configuration
//    .GetSection("OpenTelemetry")
//    .Get<OpenTelemetryOptions>() ?? default!;

//if (builder.Configuration.GetValue<bool>("ObservabilityEnabled"))
//{
//    openTelemetryOptions.Environment = builder.Environment.EnvironmentName;
//    builder.Services.AddOpenTelemetryMonitoring(openTelemetryOptions);
//    builder.Services.AddSingleton(new ActivitySource(openTelemetryOptions.ServiceName));
//}
//else
//{
//    builder.Services.AddSingleton(new ActivitySource(openTelemetryOptions.ServiceName));
//}

//builder.Services.AddHostedService<QuartzJobHostedService>();

//var host = builder.Build();

//await host.RunAsync();


namespace Etl.netDemo
{

    public static class SalesAggregationProcess
    {
        public static void DefineProcess(ISingleStream<Config> contextStream)
        {
            // 0) Prepare once & share (forces early, single evaluation)
            var prepared = contextStream
                .Select("prepare range", cfg =>
                {
                    var (start, end) = GetDateRange(cfg.ProcessDate, cfg.AggregationType);
                    Console.WriteLine($"[prep] {cfg.AggregationType} {start:yyyy-MM-dd}..{end:yyyy-MM-dd}");
                    return new
                    {
                        cfg.SourceConnectionString,
                        cfg.AggregationType,
                        StartDate = start,
                        EndDate = end
                    };
                });

            // 1) Extract
            var salesStream = prepared.CrossApply("load sales (source)",
                r => ReadSales(r.SourceConnectionString, r.StartDate, r.EndDate));

            // 2) Attach config + compute keys
            var keyed = salesStream.Select("attach cfg + keys", prepared, (sale, p) => new
            {
                Sale = sale,
                p.AggregationType,
                PeriodKey = GetPeriodKey(sale.SaleDate, p.AggregationType),
                AggregationDate = GetAggregationDate(sale.SaleDate, p.AggregationType)
            });

            // 3) Aggregate by (ItemCode, PeriodKey, AggregationType)
            var aggregated = keyed.Aggregate(
                "aggregate",
                x => new { x.Sale.ItemCode, x.PeriodKey, x.AggregationType },
                x => new SalesAggregation
                {
                    AggregationDate = x.AggregationDate,
                    AggregationType = x.AggregationType.ToUpperInvariant(),
                    ItemCode = x.Sale.ItemCode,
                    TotalAmount = 0m,
                    TransactionCount = 0,
                    AverageAmount = 0m,
                    ProcessedAt = DateTime.UtcNow,
                    PeriodKey = x.PeriodKey
                },
                (acc, v) =>
                {
                    acc.TotalAmount += v.Sale.Amount;
                    acc.TransactionCount++;
                    acc.AverageAmount = acc.TotalAmount / acc.TransactionCount;
                    return acc;
                });

            // 4) Project payload out of AggregationResult
            var toSave = aggregated
                .Select("payload", r => r.Aggregation)
                .Do("trace", a =>
                    Console.WriteLine($"[save] {a.AggregationType} {a.ItemCode} {a.PeriodKey} -> " +
                                      $"{a.TransactionCount} tx, Total {a.TotalAmount:F2}"));

            // 5) Upsert to target
            toSave.SqlServerSave("upsert SalesAggregation", o => o
                .ToTable("dbo.SalesAggregation")
                .SeekOn(a => new { a.AggregationType, a.ItemCode, a.PeriodKey }));
        }

        // Stream reader from source database
        private static IEnumerable<Sale> ReadSales(string connectionString, DateTime start, DateTime end)
        {
            using var cn = new SqlConnection(connectionString);
            cn.Open();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = @"
                SELECT Id, SaleDate, ItemCode, Amount
                FROM dbo.Sales
                WHERE SaleDate >= @StartDate AND SaleDate < @EndDate"; // half-open window
            cmd.Parameters.Add(new SqlParameter("@StartDate", SqlDbType.DateTime2) { Value = start });
            cmd.Parameters.Add(new SqlParameter("@EndDate", SqlDbType.DateTime2) { Value = end });

            using var rdr = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
            int idxId = rdr.GetOrdinal("Id");
            int idxDate = rdr.GetOrdinal("SaleDate");
            int idxItem = rdr.GetOrdinal("ItemCode");
            int idxAmt = rdr.GetOrdinal("Amount");

            while (rdr.Read())
            {
                yield return new Sale
                {
                    Id = rdr.GetInt32(idxId),
                    SaleDate = rdr.GetDateTime(idxDate),
                    ItemCode = rdr.GetString(idxItem),
                    Amount = rdr.GetDecimal(idxAmt)
                };
            }
        }

        // Helpers
        private static (DateTime start, DateTime end) GetDateRange(DateTime d, string kind)
        {
            var k = kind.ToUpperInvariant();
            if (k == "DAILY")
            {
                var start = d.Date;
                return (start, start.AddDays(1));
            }
            if (k == "WEEKLY")
            {
                var start = StartOfWeek(d, DayOfWeek.Monday);
                return (start, start.AddDays(7));
            }
            if (k == "MONTHLY")
            {
                var start = new DateTime(d.Year, d.Month, 1);
                return (start, start.AddMonths(1));
            }
            throw new ArgumentException($"Invalid aggregation type: {kind}");
        }

        private static DateTime GetAggregationDate(DateTime dt, string kind) =>
            kind.ToUpperInvariant() switch
            {
                "DAILY" => dt.Date,
                "WEEKLY" => StartOfWeek(dt, DayOfWeek.Monday),
                "MONTHLY" => new DateTime(dt.Year, dt.Month, 1),
                _ => throw new ArgumentException($"Invalid aggregation type: {kind}")
            };

        private static string GetPeriodKey(DateTime dt, string kind) =>
            kind.ToUpperInvariant() switch
            {
                "DAILY" => dt.ToString("yyyy-MM-dd"),
                "WEEKLY" =>
                    $"{dt.Year}-W{CultureInfo.InvariantCulture.Calendar.GetWeekOfYear(dt, CalendarWeekRule.FirstFourDayWeek, DayOfWeek.Monday):D2}",
                "MONTHLY" => dt.ToString("yyyy-MM"),
                _ => throw new ArgumentException($"Invalid aggregation type: {kind}")
            };

        private static DateTime StartOfWeek(DateTime dt, DayOfWeek start)
        {
            int diff = (7 + (dt.DayOfWeek - start)) % 7;
            return dt.Date.AddDays(-diff);
        }
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            var cfg = new Config
            {
                SourceConnectionString = "Server=.\\SQLEXPRESS;Database=SalesDb;Trusted_Connection=True;Encrypt=True;TrustServerCertificate=True;MultipleActiveResultSets=true;",
                TargetConnectionString = "Server=.\\SQLEXPRESS;Database=ReportDb;Trusted_Connection=True;Encrypt=True;TrustServerCertificate=True;MultipleActiveResultSets=true;",
                ProcessDate = DateTime.Today.AddDays(-1),
                AggregationType = "WEEKLY"
            };

            var runner = StreamProcessRunner.Create<Config>(SalesAggregationProcess.DefineProcess);

            // Inject TARGET connection for SqlServerSave
            using var targetCnx = new SqlConnection(cfg.TargetConnectionString);
            await targetCnx.OpenAsync();

            var execOpts = new ExecutionOptions<Config>
            {
                Resolver = new SimpleDependencyResolver().Register(targetCnx),
                TraceProcessDefinition = DefineTraceProcess,
                UseDetailedTraces = true
            };

            Console.WriteLine($"Starting {cfg.AggregationType} aggregation for {cfg.ProcessDate:yyyy-MM-dd}...");
            await runner.ExecuteAsync(cfg, execOpts);
            Console.WriteLine("ETL process completed successfully!");
        }

        static void DefineTraceProcess(IStream<TraceEvent> trace, ISingleStream<Config> _)
        {
            trace.Do("print traces", t =>
            {
                // You'll see node completions and any unhandled exceptions here
                Console.WriteLine($"[trace] {t.DateTime:HH:mm:ss.fff} {t.NodeName} {t.NodeTypeName} {t.Content}");
            });
        }
    }

}
