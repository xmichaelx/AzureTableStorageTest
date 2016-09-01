using Microsoft.WindowsAzure.Storage.Table;
using System;

namespace TableStorageWrapper
{
    public class TableItem : TableEntity
    {
        public TableItem(Item item)
        {
            PartitionKey = item.PrimaryId + "$" + item.SecondaryId;
            var ticks = item.ProductionTime.UtcTicks; 
            RowKey = ticks.ToString("d20") + "$" + Guid.NewGuid();
            Payload = item.Payload;
        }

        public TableItem() { }

        public string Payload { get; set; }

        public Item ToItem()
        {
            var item = new Item
            {
                Payload = Payload
            };

            var ids = PartitionKey.Split('$');
            var productionTimeTicks = long.Parse(RowKey.Split('$')[0]);

            item.PrimaryId = ids[0];
            item.SecondaryId = ids[1];
            item.ProductionTime = new DateTimeOffset(productionTimeTicks, TimeSpan.Zero);

            return item;
        }
    }
}
