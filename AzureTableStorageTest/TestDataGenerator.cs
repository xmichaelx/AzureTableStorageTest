using System;
using System.Collections.Generic;
using TableStorageWrapper;

namespace AzureTableStorageTest
{
    public class TestDataGenerator
    {

        public static List<Item> GenerateData(DateTimeOffset from, DateTimeOffset to, int recordsCount, int payloadLength)
        {
            var list = new List<Item>(recordsCount);
            var dt = (to - from).TotalMilliseconds / recordsCount;

            for (var i = 0; i < recordsCount; i++)
            {
                list.Add(new Item
                {
                    PrimaryId = "primaryid",
                    SecondaryId = "secondaryId0" + (i % 20), // to spread equally on 20 partitions
                    ProductionTime = from + TimeSpan.FromMilliseconds(i*dt),
                    Payload = new string('x',payloadLength)
                });
            }

            return list;
        }

    }
}
