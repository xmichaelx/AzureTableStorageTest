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
            using (var wrapper = new Wrapper(accountName, accountKey))
            {
                // generate test data entities
                Stopwatch timer = new Stopwatch();
                timer.Start();
                var testData = TestDataGenerator.GenerateData(DateTimeOffset.UtcNow.AddDays(-30), DateTimeOffset.UtcNow, size, 300).Select(x => new TableItem(x)).ToList();
                timer.Stop();
                Console.WriteLine($"Creating {size} items elapsed {timer.Elapsed} s");
                timer.Restart();
                wrapper.AddBatch(testData);
                timer.Stop();
                Console.WriteLine($"Data upload elapsed {timer.Elapsed} s - {100 * timer.ElapsedMilliseconds / size} ms per batch request");
                // querying



                // deleting
                timer.Restart();
                wrapper.DeleteItemsOlderThan(DateTimeOffset.UtcNow);
                timer.Stop();
                Console.WriteLine($"Data delete elapsed {timer.Elapsed} s - {100 * timer.ElapsedMilliseconds / size} ms per batch request");
            }
        }
    }
}
