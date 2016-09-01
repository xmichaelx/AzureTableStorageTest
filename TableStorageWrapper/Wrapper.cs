using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace TableStorageWrapper
{
    public class Wrapper : IDisposable
    {
        private CloudStorageAccount storageAccount;
        private CloudTableClient tableClient;

        private CloudTable testTable;

        public Wrapper(string accountName, string accountKey)
        {
            var connectionString = $"DefaultEndpointsProtocol=https;AccountName={accountName};AccountKey={accountKey}";
            storageAccount = CloudStorageAccount.Parse(connectionString);
            //ServicePoint tableServicePoint = ServicePointManager.FindServicePoint(storageAccount.TableEndpoint);
            //tableServicePoint.UseNagleAlgorithm = false;

            tableClient = storageAccount.CreateCloudTableClient();
            tableClient.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.JsonNoMetadata;
            testTable = tableClient.GetTableReference("test");
            testTable.CreateIfNotExists();
        }

        public static string BetweenDates(DateTimeOffset from, DateTimeOffset to)
        {
            var c1 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, from.UtcTicks.ToString("d20"));
            var c2 = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, to.UtcTicks.ToString("d20"));
            return TableQuery.CombineFilters(c1, TableOperators.And, c2);
        }

        public List<Item> GetItems(string primaryId, string secondaryId, DateTimeOffset from, DateTimeOffset to)
        {
            var c3 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, $"{primaryId}#{secondaryId}");

            TableQuery <TableItem> rangeQuery = new TableQuery<TableItem>()
                .Where(TableQuery.CombineFilters(BetweenDates(from, to), TableOperators.And, c3)).Select(new List<string> { "Payload" });


            return testTable.ExecuteQuery(rangeQuery).Select(x => x.ToItem()).ToList();
        }

        public List<Item> GetItems(string primaryId,  DateTimeOffset from, DateTimeOffset to)
        {
            // to select only those with specific primaryId
            var c3 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, $"{primaryId}$");
            var c4 = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, $"{primaryId}%");


            TableQuery<TableItem> rangeQuery = new TableQuery<TableItem>()
                .Where(TableQuery.CombineFilters(BetweenDates(from, to), TableOperators.And, TableQuery.CombineFilters(c4, TableOperators.And, c3))).Select(new List<string> { "Payload" });


            return testTable.ExecuteQuery(rangeQuery).Select(x => x.ToItem()).ToList();
        }

        public void AddBatch(List<TableItem> items)
        {
            var totalBatchOperations = items
                .GroupBy(x => x.PartitionKey).Select(group =>
            {
                var batchOperations = new List<TableBatchOperation>();
                var batchOperation = new TableBatchOperation();
                foreach (var e in group)
                {
                    batchOperation.InsertOrReplace(e);
                    if (batchOperation.Count == 100)
                    {
                        batchOperations.Add(batchOperation);
                        batchOperation = new TableBatchOperation();
                    }
                }

                if (batchOperation.Count > 0)
                {
                    batchOperations.Add(batchOperation);
                }

                return batchOperations;
            }).SelectMany(x => x).ToList();


            Parallel.ForEach(totalBatchOperations, batchOperation =>
            {
                testTable.ExecuteBatchAsync(batchOperation).Wait();
            });
        }

        public async void Add(Item item)
        {
            var tableItem = new TableItem(item);
            TableOperation op = TableOperation.InsertOrReplace(tableItem);
            await testTable.ExecuteAsync(op);
        }

        public void DeleteItemsOlderThan(DateTimeOffset date)
        {

            var projectionQuery = new TableQuery<DynamicTableEntity>()
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, date.UtcTicks.ToString("d20")))
                .Select(new string[] { "RowKey" });


            var totalBatchOperations = testTable.ExecuteQuery(projectionQuery).GroupBy(x => x.PartitionKey).Select(group =>
            {
                var batchOperations = new List<TableBatchOperation>();
                var batchOperation = new TableBatchOperation();
                foreach (var e in group)
                {
                    batchOperation.Delete(e);
                    if (batchOperation.Count == 100)
                    {
                        batchOperations.Add(batchOperation);
                        batchOperation = new TableBatchOperation();
                    }
                }

                if (batchOperation.Count > 0)
                {
                    batchOperations.Add(batchOperation);
                }

                return batchOperations;
            }).SelectMany(x => x).ToList();

            Parallel.ForEach(totalBatchOperations, batchOperation =>
            {
                testTable.ExecuteBatchAsync(batchOperation).Wait();
            });
        }

        public void Dispose()
        {
            testTable.DeleteIfExists();
        }
    }
}
