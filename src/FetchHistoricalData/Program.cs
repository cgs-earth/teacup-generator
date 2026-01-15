using RiseTeacupsLib;
using System.Text.Json;

namespace FetchHistoricalData
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

            Directory.CreateDirectory("historicalData");

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

            foreach (Location location in locationList)
            {
                try
                {
                    Console.WriteLine($"Querying Data: {location.SiteName}");
                    location.DataClient.FetchData(startDate, endDate, includeRise, useTest);

                    if (location.DataClient.Data.Count > 0)
                    {
                        string jsonString = JsonSerializer.Serialize(location.DataClient.Data);
                        File.WriteAllText($"historicalData/{location.SiteName}.json", jsonString);
                    }
                    else
                    {
                        Console.WriteLine($"Skipping {location.SiteName}: No historical data found");
                    }
                }
                catch
                {
                    // skip
                }
            }

            timer.Stop();
            Console.WriteLine($"Total execution time: {timer.Elapsed}");
        }
    }
}
