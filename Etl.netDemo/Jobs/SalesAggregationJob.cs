using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Quartz;
using Tps.Dms.Shared.Constants;
using Tps.Dms.Shared.Utilities.Abstractions.Cqrs;

namespace Etl.netDemo.Jobs
{
    /// <summary>
    /// Defines the <see cref="SalesAggregationJob" /> which is responsible 
    /// for marking major units as available automatically.
    /// </summary>
    public class SalesAggregationJob(ILogger<SalesAggregationJob> logger) : IJob
    {

        private readonly ILogger<SalesAggregationJob> _logger = logger;

        /// <summary>
        /// Executes the job to mark major units as available.
        /// </summary>
        /// <param name="context">The context<see cref="IJobExecutionContext"/></param>
        /// <returns>The <see cref="Task"/></returns>
        public async Task Execute(IJobExecutionContext context)
        {
            var jobData = JsonConvert.DeserializeObject<Dictionary<string, string>>(context.Trigger.JobDataMap.GetString(DmsJobDataMap.JobData) ?? string.Empty);
            _logger.LogInformation("Started Job");
        }
    }
}
