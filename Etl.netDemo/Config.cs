using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Etl.netDemo
{
    // Source data model (from DB1.Sales table)
    public class Sale
    {
        public int Id { get; set; }
        public DateTime SaleDate { get; set; }
        public string ItemCode { get; set; }
        public decimal Amount { get; set; }
    }

    // Aggregated data model (for DB2 target table)
    public class SalesAggregation
    {
        public DateTime AggregationDate { get; set; }
        public string AggregationType { get; set; } // DAILY, WEEKLY, MONTHLY
        public string ItemCode { get; set; }
        public decimal TotalAmount { get; set; }
        public int TransactionCount { get; set; }
        public decimal AverageAmount { get; set; }
        public DateTime ProcessedAt { get; set; }
        public string PeriodKey { get; set; } // e.g., "2025-W33" for weekly, "2025-08" for monthly
    }


    // Configuration class moved to the top level
    public class Config
    {
        public string SourceConnectionString { get; set; }
        public string TargetConnectionString { get; set; }
        public DateTime ProcessDate { get; set; }
        public string AggregationType { get; set; } // DAILY, WEEKLY, MONTHLY
    }


}
