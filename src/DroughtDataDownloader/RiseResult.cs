using System;
using System.Collections.Generic;

namespace DroughtDataDownloader
{
    class RiseResult
    {
        public Link links { get; set; }
        public Meta meta { get; set; }
        public List<Data> data { get; set; }
    }

    class Link
    {
        public string self;
        public string first;
        public string last;
        public string next;
    }

    class Meta
    {
        public int totalItems;
        public int itemsPerPage;
        public int currentPage;
    }

    class Data
    {
        public string id;
        public string type;
        public Attributes attributes;
    }

    class Attributes
    {
        public int? _id = null;
        public int? itemId = null;
        public int? locationId = null;
        public string sourceCode;
        public DateTime dateTime;
        public float? result;
        public string status;
        public int? modelRunMemberId = null;
        public int? parameterId = null;
        public int? modelRunId = null;
        public ResultAttributes resultAttributes;
        public DateTime? lastUpdate;
        public DateTime createDate;
        public DateTime? updateDate;
    }

    class ResultAttributes
    {
        public string timeStep;
        public string resultType;
    }
}
