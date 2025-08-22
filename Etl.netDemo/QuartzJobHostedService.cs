using Etl.netDemo.Jobs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Quartz;
using TimeZoneConverter;
using Tps.Dms.Shared.Constants;
using Tps.Dms.Shared.Utilities.Application.Jobs;

namespace Etl.netDemo
{
    /// <summary>
    /// Hosted service that initializes and schedules jobs at application startup.
    /// </summary>
    public class QuartzJobHostedService : IHostedService
    {
        private readonly ISchedulerFactory _schedulerFactory;
        private readonly IConfiguration _configuration;

        /// <summary>
        /// Defines the StoreSpecificJobs
        /// </summary>
        private List<DmsJobDefinition> StoreSpecificJobs =>
            [
                new DmsJobDefinition(nameof(SalesAggregationJob), typeof(SalesAggregationJob), _configuration.GetValue<string>("JobCronExpressions:SalesAggregationJob") ?? string.Empty),
            ];

        public QuartzJobHostedService(
            ISchedulerFactory schedulerFactory,
            IConfiguration configuration)
        {
            _schedulerFactory = schedulerFactory;
            _configuration = configuration;
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            var scheduler = await _schedulerFactory.GetScheduler(default);
            await GenerateTriggers(scheduler, new StoreInfo()
            {
                OrganizationId = 100001,
                TenantId = "100001",
                TimeZoneId = "Asia/Kolkata"
            });
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            //if (_scheduler != null)
            //{
            //    await _scheduler.Shutdown(waitForJobsToComplete: true, cancellationToken);
            //}
        }

        /// <summary>
        /// The GenerateTriggers
        /// </summary>
        /// <param name="scheduler">The scheduler<see cref="IScheduler"/></param>
        /// <param name="storeInfo">The eventData<see cref="StoreInfo"/></param>
        /// <returns>The <see cref="Task"/></returns>
        private async Task GenerateTriggers(IScheduler scheduler, StoreInfo storeInfo)
        {
            foreach (var job in StoreSpecificJobs)
            {
                await EnsureJobExists(scheduler, job);
                string triggerName = job.GetTriggerName(storeInfo.OrganizationId, storeInfo.TenantId);
                var trigger = await scheduler.GetTrigger(new TriggerKey(triggerName));
                if (trigger == null)
                {
                    var newTriggerBuilder = TriggerBuilder.Create()
                        .WithIdentity(triggerName)
                        .WithCronSchedule(job.Cron, act => act.InTimeZone(TZConvert.GetTimeZoneInfo(storeInfo.TimeZoneId)))
                        .ForJob(job.JobName);

                    if (job.JobDataMap != null)
                    {
                        newTriggerBuilder = newTriggerBuilder.UsingJobData(job.JobDataMap);
                    }

                    var newTrigger = newTriggerBuilder
                        .UsingJobData(DmsJobDataMap.OrganizationId, storeInfo.OrganizationId.ToString()) // Quartz persistence storage only supports string 
                        .UsingJobData(DmsJobDataMap.TenantId, storeInfo.TenantId)
                        .UsingJobData(DmsJobDataMap.JobData, JsonConvert.SerializeObject(new Dictionary<string, string> { { "TimeZoneId", storeInfo.TimeZoneId } }))
                        .Build();

                    await scheduler.ScheduleJob(newTrigger);
                }
                else
                {
                    var newTrigger = trigger.GetTriggerBuilder()
                        .UsingJobData(DmsJobDataMap.JobData, JsonConvert.SerializeObject(new Dictionary<string, string> { { "TimeZoneId", storeInfo.TimeZoneId } }))
                        .WithCronSchedule(job.Cron, act => act.InTimeZone(TZConvert.GetTimeZoneInfo(storeInfo.TimeZoneId)))
                        .Build();

                    await scheduler.RescheduleJob(trigger.Key, newTrigger);
                }
            }
        }

        private static async Task EnsureJobExists(IScheduler scheduler, DmsJobDefinition job)
        {
            var jobKey = new JobKey(job.JobName);
            if (!await scheduler.CheckExists(jobKey))
            {
                var jobDetail = JobBuilder.Create(job.JobType)
                    .WithIdentity(jobKey)
                    .StoreDurably()
                    .Build();

                await scheduler.AddJob(jobDetail, true);
            }
        }
    }

    public class StoreInfo
    {
        public int OrganizationId { get; set; }
        public string TenantId { get; set; }
        public string TimeZoneId { get; set; }
    }
}
