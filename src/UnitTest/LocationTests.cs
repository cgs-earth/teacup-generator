using RiseTeacupsLib;

namespace UnitTest
{
    [TestClass]
    public class LocationTests
    {
        [TestMethod]
        public void TestFromCsv()
        {
            List<Location> locations = File.ReadAllLines(@"dataDownloadTest.csv")
                .Skip(1)
                .Select(v => Location.FromCsv(v, DateTime.Today))
                .ToList();

            Assert.AreEqual(2, locations.Count);

            // mock ds location
            Location mockDsLocation = locations[0];
            Assert.AreEqual("mock_ds", mockDsLocation.DataSource);
            Assert.IsNull(mockDsLocation.RiseLocationId);
            Assert.AreEqual(DateTime.Today, mockDsLocation.RunDate);
            Assert.IsNotNull(mockDsLocation.DataClient);

            // mock rise location
            Location mockRiseLocation = locations[1];
            Assert.AreEqual("rise", mockRiseLocation.DataSource);
            Assert.AreEqual(123, mockRiseLocation.RiseLocationId);
            Assert.AreEqual(DateTime.Today, mockRiseLocation.RunDate);
            Assert.IsNotNull(mockRiseLocation.DataClient);
        }


        [TestMethod]
        public void TestGenerateCsvLine()
        {
            string line = File.ReadAllLines(@"dataDownloadTest.csv")[1];
            Location l = Location.FromCsv(line, DateTime.Today);

            Assert.AreEqual("test site name", l.SiteName);
            Assert.IsNotNull(l.DataClient);

            l.GetStatsFromCsv(@"dataDownloadStatsTest.csv");
            Assert.IsNotNull(l.Stats);
            Assert.AreEqual(55555, l.Stats.P50);

            Dictionary<DateTime, double> mockData = new Dictionary<DateTime, double>()
            {
                { DateTime.Today, 12345 },
                { DateTime.Today.AddDays(-1), 23456 },
                { DateTime.Today.AddDays(-2), 34567 }
            };

            l.DataClient.Data = mockData;

            string csvLine = l.GenerateCsvLine();
            string[] csvValues = csvLine.Split(',');

            Assert.AreEqual(26, csvValues.Count());
            Assert.AreEqual(l.SiteName, csvValues[0]);
            Assert.AreEqual(l.State, csvValues[3].Trim());
            Assert.AreEqual(12345, Convert.ToDouble(csvValues[7]));
            Assert.AreEqual(DateTime.Today, DateTime.Parse(csvValues[8]));
            Assert.AreEqual(l.Stats.Max, Convert.ToDouble(csvValues[10]));
            Assert.AreEqual(12345 / l.Stats.P50, Convert.ToDouble(csvValues[18]));
            Assert.AreEqual(12345 / l.Stats.Avg, Convert.ToDouble(csvValues[19]));
            Assert.AreEqual((double)12345 / l.MaxCapacity, Convert.ToDouble(csvValues[22]));
        }


        [TestMethod]
        public void TestGetStatsFromCsv()
        {
            string csvPath = @"dataDownloadStatsTest.csv";

            List<Location> locations = File.ReadAllLines(@"dataDownloadTest.csv")
                .Skip(1)
                .Select(v => Location.FromCsv(v, DateTime.Today))
                .ToList();

            Assert.AreEqual(2, locations.Count);

            // mock ds location
            Location mockDsLocation = locations[0];
            mockDsLocation.GetStatsFromCsv(csvPath);
            Assert.IsNotNull(mockDsLocation.Stats);
            Assert.AreEqual(100000, mockDsLocation.Stats.Max);
            Assert.AreEqual(99999, mockDsLocation.Stats.P90);
            Assert.AreEqual(123, mockDsLocation.Stats.Min);
            Assert.AreEqual(5432, mockDsLocation.Stats.Avg);

            // mock rise location
            Location mockRiseLocation = locations[1];
            mockRiseLocation.GetStatsFromCsv(csvPath);
            Assert.IsNotNull(mockRiseLocation.Stats);
            Assert.AreEqual(12345, mockRiseLocation.Stats.Max);
            Assert.AreEqual(2345, mockRiseLocation.Stats.P90);
            Assert.AreEqual(456, mockRiseLocation.Stats.Min);
            Assert.AreEqual(4567, mockRiseLocation.Stats.Avg);
        }
    }
}