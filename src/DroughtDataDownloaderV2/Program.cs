using System;
using System.Net;
using Newtonsoft.Json;
using RiseTeacupsLib;
using Location = RiseTeacupsLib.Location;

namespace DroughtDataDownloaderV2
{
    internal class Program
    {
        private static List<Location> locationList = new List<Location>();
        public static DateTime startDate;
        public static DateTime endDate;
        public static bool includeRise = false;
        public static bool useTest = false;
        static void Main(string[] args)
        {
            var timer = System.Diagnostics.Stopwatch.StartNew();

            ParseArguments(args);

            // ensure this directory exists
            Directory.CreateDirectory("datafiles");

            for (DateTime d = endDate; d >= startDate; d = d.AddDays(-1))
            {
                Console.WriteLine($"\n----- Running for {d.ToString()} -----\n");
                locationList = File.ReadAllLines(@"dataDownload.csv")
                    .Skip(1)
                    .Select(v => Location.FromCsv(v, d))
                    .ToList();

                // lines for output csv file
                List<string> outputLines = new List<string>();
                outputLines.Add("SiteName, Lat, Lon, State, DoiRegion, Huc8, DataUnits, DataValue, DataDate, DateQueried, DataDateMax, DataDateP90, DataDateP75, DataDateP50, DataDateP25, DataDateP10, DataDateMin, DataDateAvg, DataValuePctMdn, DataValuePctAvg, StatsPeriod, MaxCapacity, PctFull, TeacupUrl, DataUrl, Comment");

                foreach (Location location in locationList)
                {
                    Console.WriteLine($"\nQuerying Data: {location.SiteName}");
                    location.DataClient.FetchData(d.AddDays(-7), d, includeRise, useTest);
                    Stats stats = location.DataClient.CalculateSummaryStatistics();
                    location.Stats = stats;
                    string outLine = location.GenerateCsvLine();
                    outputLines.Add(outLine);
                }

                File.WriteAllLines(@"datafiles\droughtData" + d.ToString("yyyyMMdd") + ".csv", outputLines);
            }
            timer.Stop();
            Console.WriteLine($"Total execution time: {timer.Elapsed}");
        }

        private static void ParseArguments(string[] args)
        {
            if (args.Length == 0)
            {
                throw new ArgumentException("Please include the runDate command line argument");
            }
            startDate = DateTime.Parse(args[0]).Date; // has to be first arg
            endDate = DateTime.Parse(args[1].ToString()).Date; // has to be second arg
            includeRise = args.Any(a => a.Equals("-r"));
            useTest = args.Any(a => a.Equals("-t"));
        }
    }
}
