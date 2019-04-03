
using System;
using System.Linq;
//using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;

namespace Veracity.Monitoring.Miscellaneous.ServiceBus
{
    public class DeadLetterMessagesEntity : TableEntity
    {
        public string EntityPath => RowKey;

        public long Count { get; set; }
    }

    public static class DeadLetterMessagesChecker
    {
        [FunctionName("CheckForSubscription")]
        public static async Task<IActionResult> CheckForSubscription([HttpTrigger(AuthorizationLevel.Function, "post", Route = "checker/queue/{path:string}")]HttpRequest req, string topicPath, string subscriptionName, [Table("AzureWebJobsHostLogscommon")]CloudTable cloudTable, ILogger log)
        {
            var serviceBusConnectionString = Environment.GetEnvironmentVariable("MonitorServiceBusQueues");

            var managementClient = new ManagementClient(serviceBusConnectionString);

            var nmsp = await managementClient.GetNamespaceInfoAsync();
            var topic = await managementClient.GetTopicRuntimeInfoAsync(topicPath); ;
            var subscription = await managementClient.GetSubscriptionRuntimeInfoAsync(topic.Path, subscriptionName);
            
            var deadLetterMessagesCount = subscription.MessageCountDetails.DeadLetterMessageCount;

            log.LogInformation($"Subscription: {topic.Path}/{ subscription.SubscriptionName} (DeadLetters: {deadLetterMessagesCount})");
            
            TableQuery<DeadLetterMessagesEntity> rangeQuery = new TableQuery<DeadLetterMessagesEntity>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal,
                        nmsp.Name),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal,
                        $"{topic.Path}/{subscription.SubscriptionName}" )));
            
            var entity = (await cloudTable.ExecuteQuerySegmentedAsync(rangeQuery, null)).FirstOrDefault();

            if (entity == null)
            {
                return new JsonResult(deadLetterMessagesCount);
            }

            var newVal = deadLetterMessagesCount - entity.Count;

            if (newVal < 0)
                newVal = 0;

            entity.Count = deadLetterMessagesCount;

            TableOperation op = new TableOperation(entity, TableOperationType.InsertOrReplace);
            cloudTable.ExecuteAsync()

            return new JsonResult(newVal);
        }
    }
}
