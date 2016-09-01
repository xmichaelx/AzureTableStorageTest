using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TableStorageWrapper
{
    public class Item
    {
        public string PrimaryId { get; set; }
        public string SecondaryId { get; set; }
        public DateTimeOffset ProductionTime { get; set; }
        public string Payload { get; set; }
    }
}
