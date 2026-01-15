using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.ConstrainedExecution;

namespace RiseTeacupsLib
{
    public class RiseResult
    {
        public Link Links { get; set; }
        public Meta Meta { get; set; }
        public List<Data> Data { get; set; }
    }

    public class Link
    {
        public string Self;
        public string First;
        public string Last;
        public string Next;
    }

    public class Meta
    {
        public int TotalItems;
        public int ItemsPerPage;
        public int CurrentPage;
    }

    public class Data
    {
        public string Id;
        public string Type;
        public Attributes Attributes;
    }

    public class Attributes
    {
        public int? _Id = null;
        public int? ItemId = null;
        public int? LocationId = null;
        public string SourceCode;
        public DateTime DateTime;
        public float? Result;
        public string Status;
        public int? ModelRunMemberId = null;
        public int? ParameterId = null;
        public int? ModelRunId = null;
        public ResultAttributes? ResultAttributes;
        public DateTime? LastUpdate;
        public DateTime CreateDate;
        public DateTime? UpdateDate;
    }

    public class ResultAttributes
    {
        public string TimeStep;
        public string ResultType;
    }
}
