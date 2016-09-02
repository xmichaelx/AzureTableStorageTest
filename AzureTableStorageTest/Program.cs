using System;
using System.Diagnostics;
using System.Linq;
using TableStorageWrapper;

namespace AzureTableStorageTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var accountName = args[0];
            var accountKey = args[1];
            var size = int.Parse(args[2]);
            const int batchSize = 100;
            const int queryBatchSize = 1000;
            const int days = 30;
            using (var wrapper = new Wrapper(accountName, accountKey))
            {
                // generate test data entities
                Stopwatch timer = new Stopwatch();
                timer.Start();
                var testData = TestDataGenerator.GenerateData(DateTimeOffset.UtcNow.AddDays(-days), DateTimeOffset.UtcNow, size, 50).Select(x => new TableItem(x)).ToList();
                timer.Stop();
                Console.WriteLine($"Creating {size} items elapsed {timer.Elapsed} s");
                timer.Restart();
                wrapper.AddBatch(testData);
                timer.Stop();
                Console.WriteLine($"Data upload elapsed {timer.Elapsed} s - {timer.ElapsedMilliseconds / (size/ batchSize)} ms per batch request");
                // querying
                for (int i = 0; i < days; i++)
                {

                    timer.Restart();
                    var items = wrapper.GetItems("primaryid", DateTimeOffset.UtcNow.AddDays(-(i+1)), DateTimeOffset.UtcNow.AddDays(-i));
                    timer.Stop();
                    Console.WriteLine($"Data query for day {i}'s back retrieved: {items.Count} items and elapsed {timer.Elapsed} s - {timer.ElapsedMilliseconds / (((double)items.Count) / queryBatchSize)} ms per batch request");
                }

                // deleting
                timer.Restart();
                wrapper.DeleteItemsOlderThan(DateTimeOffset.UtcNow);
                timer.Stop();
                Console.WriteLine($"Data delete elapsed {timer.Elapsed} s - {timer.ElapsedMilliseconds / (size / batchSize)} ms per batch request");
            }
        }
    }
}
