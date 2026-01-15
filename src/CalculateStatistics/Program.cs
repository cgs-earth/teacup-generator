using System;
using System.Net;
using RiseTeacupsLib;

namespace CalculateStatistics
{
    internal class Program
    {
        private static List<Location> locationList = new List<Location>();
        public static DateTime startDate = new DateTime(1990, 10, 1);
        public static DateTime endDate = new DateTime(2020, 9, 30);
        public static bool includeRise = false;
        public static bool useTest = false;
        static void Main(string[] args)
        {
            var timer = System.Diagnostics.Stopwatch.StartNew();

            includeRise = args.Any(a => a.Equals("-r"));
            useTest = args.Any(a => a.Equals("-t"));

            try
            {
                // read dataDownload.csv and parse into a list of Locations
                locationList = File.ReadAllLines(@"dataDownload.csv")
                    .Skip(1)
                    .Select(v => Location.FromCsv(v, DateTime.Today))
                    .ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error parsing dataDownload.csv");
                Console.WriteLine(ex.ToString());
                return;
            }

            List<string> outLines = new List<string>();
            outLines.Add("SiteName, DataDateMax, DataDateP90, DataDateP75, DataDateP50, DataDateP25, DataDateP10, DataDateMin, DataDateAvg");
            foreach (Location location in locationList)
            {
                try
                {
                    Console.WriteLine($"Querying Data: {location.SiteName}");
                    location.DataClient.FetchData(startDate, endDate, includeRise, useTest);
                    Stats stats = location.DataClient.CalculateSummaryStatistics();
                    location.Stats = stats;
                    string outLine = "";
                    outLine += $"{location.SiteName}, ";
                    outLine += $"{stats.Max}, ";
                    outLine += $"{stats.P90}, ";
                    outLine += $"{stats.P75}, ";
                    outLine += $"{stats.P50}, ";
                    outLine += $"{stats.P25}, ";
                    outLine += $"{stats.P10}, ";
                    outLine += $"{stats.Min}, ";
                    outLine += $"{stats.Avg}, ";
                    outLines.Add(outLine);
                }
                catch
                {
                    // skip
                }
            }

            File.WriteAllLines("dataDownloadStats.csv", outLines);
            timer.Stop();
            Console.WriteLine($"Total execution time: {timer.Elapsed}");
        }
    }
}
