using RiseTeacupsLib;
using Moq;
using Moq.Protected;
using System.Net;

namespace UnitTest
{
    [TestClass]
    public class DataClientTests
    {
        private string BuildMockResponseContent(Location location)
        {
            string content = "";
            if (location.RiseLocationId != null)
            {
                content = $@"{{""links"":{{""self"":""riselink.test""}},""meta"":{{""totalItems"":2,""itemsPerPage"":10000,""currentPage"":1}},""data"":[{{""id"":""riseid"",""type"":""Result"",""attributes"":{{""_id"":12345,""itemId"":76,""locationId"":1,""sourceCode"":""uchdb2"",""dateTime"":""{DateTime.Today.AddDays(-1)}"",""result"":112233,""status"":null,""modelRunMemberId"":null,""parameterId"":3,""modelRunId"":null,""resultAttributes"":{{""timeStep"":""day"",""resultType"":""observed""}},""lastUpdate"":""2022-12-08T20:13:00+00:00"",""createDate"":""2020-07-22T07:30:09+00:00"",""updateDate"":""2022-12-10T19:10:59+00:00""}}}},{{""id"":""riseid"",""type"":""Result"",""attributes"":{{""_id"":12345,""itemId"":76,""locationId"":1,""sourceCode"":""uchdb2"",""dateTime"":""{DateTime.Today}"",""result"":98765,""status"":null,""modelRunMemberId"":null,""parameterId"":3,""modelRunId"":null,""resultAttributes"":{{""timeStep"":""day"",""resultType"":""observed""}},""lastUpdate"":""2022-12-08T20:13:00+00:00"",""createDate"":""2020-07-22T07:30:09+00:00"",""updateDate"":""2022-12-10T19:10:59+00:00""}}}}]}}";
            }
            else
            {
                switch (location.DataSource)
                {
                    case "usgs":
                        content = $"agency_cd\tsite_no\tdatetime\t113135_72275_00003\t113135_72275_00003_cd\n5s 15s 20d 14n 10s\nUSGS 11507001 {DateTime.Today.ToString("yyyy-MM-dd")} 4200 P\nUSGS 11507001 {DateTime.Today.AddDays(-1).ToString("yyyy-MM-dd")} 4100 P";
                        break;
                    case "pnhyd":
                        content = $"DateTime,jck_af\r\n{DateTime.Today.AddDays(-1).ToString("yyyy-MM-dd")},12345.00\r\n{DateTime.Today.ToString("yyyy-MM-dd")},99999.15";
                        break;
                    case "gphyd":
                        content = $"DateTime,jck_af\r\n{DateTime.Today.AddDays(-1).ToString("yyyy-MM-dd")},12345.00\r\n{DateTime.Today.ToString("yyyy-MM-dd")},99999.15";
                        break;
                    case "cdec":
                        content = $"STATION_ID,DURATION,SENSOR_NUMBER,SENSOR_TYPE,DATE TIME,OBS DATE,VALUE,DATA_FLAG,UNITS\r\nTEST,D,15,STORAGE,{DateTime.Today.AddDays(-1).ToString("yyyyMMdd")} 0000,{DateTime.Today.AddDays(-1).ToString("yyyyMMdd")} 0000,987654, ,AF\r\nTEST,D,15,STORAGE,{DateTime.Today.ToString("yyyyMMdd")} 0000,{DateTime.Today.ToString("yyyyMMdd")} 0000,123450, ,AF";
                        break;
                    case "uchdb2":
                        content = $"Date,SDI 1714: SITE NAME - STORAGE END OF PERIOD READING in ACRE-FEET\n{DateTime.Today.AddDays(-1).ToString("MM/dd/yyyy")} 00:00, 12345.01\n{DateTime.Today.ToString("MM/dd/yyyy")} 00:00, 78945.321";
                        break;
                    case "lchdb":
                        content = $"Date,SDI 1714: SITE NAME - STORAGE END OF PERIOD READING in ACRE-FEET\n{DateTime.Today.AddDays(-1).ToString("MM/dd/yyyy")} 00:00, 12345.01\n{DateTime.Today.ToString("MM/dd/yyyy")} 00:00, 78945.321";
                        break;
                    default:
                        break;
                }
            }
            return content;
        }
        private Mock<HttpMessageHandler> BuildMockHttpHandler(string content)
        {
            var mockHttpHandler = new Mock<HttpMessageHandler>(MockBehavior.Default);
            mockHttpHandler
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>()
                )
                .ReturnsAsync(new HttpResponseMessage()
                {
                    StatusCode = HttpStatusCode.OK,
                    Content = new StringContent(content),
                })
                .Verifiable();

            return mockHttpHandler;
        }


        [TestMethod]
        public void TestDecorateWithRetry()
        {
            DataClient dc = new DataClient(new Location(), new HttpClient());
            // test default generic values
            object val = dc.DecorateWithRetry<object>(void () => throw new Exception("testing"));
            Assert.IsNull(val);
            RiseResult r = dc.DecorateWithRetry<RiseResult>(void () => throw new Exception("testing"));
            Assert.IsNull(r);
        }


        [TestMethod]
        public void TestGetUsgsData()
        {
            Location l = new Location();
            l.DataSource = "usgs";
            string mockContent = BuildMockResponseContent(l);
            var handler = BuildMockHttpHandler(mockContent);
            DataClient dc = new DataClient(l, new HttpClient(handler.Object));
            dc.FetchData(DateTime.Today.AddDays(-7), DateTime.Today, false, false);
            Assert.AreEqual(2, dc.Data.Count);
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today));
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today.AddDays(-1)));
            Assert.AreEqual(0.0, dc.Data[DateTime.Today.AddDays(-1)]);
            Assert.AreEqual(580110.00, dc.Data[DateTime.Today]);
        }


        [TestMethod]
        public void TestGetHdbData()
        {
            Location l = new Location();
            l.DataSource = "uchdb2";
            string mockContent = BuildMockResponseContent(l);
            var handler = BuildMockHttpHandler(mockContent);
            DataClient dc = new DataClient(l, new HttpClient(handler.Object));
            dc.FetchData(DateTime.Today.AddDays(-7), DateTime.Today, false, false);
            Assert.AreEqual(2, dc.Data.Count);
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today));
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today.AddDays(-1)));
            Assert.AreEqual(12345.01, dc.Data[DateTime.Today.AddDays(-1)]);
            Assert.AreEqual(78945.321, dc.Data[DateTime.Today]);
        }


        [TestMethod]
        public void TestGetHydrometData()
        {
            Location l = new Location();
            l.DataSource = "pnhyd";
            string mockContent = BuildMockResponseContent(l);
            var handler = BuildMockHttpHandler(mockContent);
            DataClient dc = new DataClient(l, new HttpClient(handler.Object));
            dc.FetchData(DateTime.Today.AddDays(-7), DateTime.Today, false, false);
            Assert.AreEqual(2, dc.Data.Count);
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today));
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today.AddDays(-1)));
            Assert.AreEqual(12345.00, dc.Data[DateTime.Today.AddDays(-1)]);
            Assert.AreEqual(99999.15, dc.Data[DateTime.Today]);
        }


        [TestMethod]
        public void TestGetCdecData()
        {
            Location l = new Location();
            l.DataSource = "cdec";
            l.DataIdentifier = "test";
            string mockContent = BuildMockResponseContent(l);
            var handler = BuildMockHttpHandler(mockContent);
            DataClient dc = new DataClient(l, new HttpClient(handler.Object));
            dc.FetchData(DateTime.Today.AddDays(-7), DateTime.Today, false, false);
            Assert.AreEqual(2, dc.Data.Count);
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today));
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today.AddDays(-1)));
            Assert.AreEqual(987654, dc.Data[DateTime.Today.AddDays(-1)]);
            Assert.AreEqual(123450, dc.Data[DateTime.Today]);
        }


        [TestMethod]
        public void TestGetRiseData()
        {
            Location l = new Location();
            l.DataSource = "rise";
            l.SiteName = "rise test site";
            l.RiseLocationId = 1;
            string mockContent = BuildMockResponseContent(l);
            var handler = BuildMockHttpHandler(mockContent);
            DataClient dc = new DataClient(l, new HttpClient(handler.Object));
            dc.FetchData(DateTime.Today.AddDays(-7), DateTime.Today, true, false);
            Assert.AreEqual(2, dc.Data.Count);
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today));
            Assert.IsTrue(dc.Data.ContainsKey(DateTime.Today.AddDays(-1)));
            Assert.AreEqual(112233, dc.Data[DateTime.Today.AddDays(-1)]);
            Assert.AreEqual(98765, dc.Data[DateTime.Today]);
        }


        [TestMethod]
        public void TestGetMostRecentValue()
        {
            Location l = new Location();
            DataClient dc = new DataClient(l, new HttpClient());
            DateTime date = DateTime.Today;
            for (double i = 0; i <= 7; i++)
            {
                date = date.AddDays(-i);
                dc.Data.Add(date, i);
            }
            KeyValuePair<DateTime, string> result = dc.GetMostRecentValue(DateTime.Today);
            Assert.AreNotEqual("NaN", result.Value);
            Assert.AreEqual(0.ToString(), result.Value);

            dc.Data = new Dictionary<DateTime, double>();
            result = dc.GetMostRecentValue(DateTime.Today);
            Assert.AreEqual("NaN", result.Value); // should return NaN if no data
        }


        [TestMethod]
        public void TestGetKlamathStorage()
        {
            double v1 = DataClient.GetKlamathStorage(4000);
            Assert.AreEqual(0, v1);
            double v2 = DataClient.GetKlamathStorage(5000);
            Assert.AreEqual(580110.00, v2);
            double v3 = DataClient.GetKlamathStorage(4136.02);
            Assert.AreEqual(1318.00, v3);
        }


        [TestMethod]
        public void TestCalculateSummaryStatistics()
        {
            Location l = new Location();
            l.SiteName = "TestLocation";
            l.RunDate = new DateTime(2024, 11, 1);
            DataClient dc = new DataClient(l, new HttpClient());
            Stats stats = dc.CalculateSummaryStatistics();
            Assert.IsNotNull(stats);
            Assert.AreEqual(5, stats.Min);
            Assert.AreEqual(120, stats.Max);
            Assert.AreEqual(32.4, stats.Avg);
            Assert.AreEqual(120, stats.P90);
            Assert.AreEqual(12, stats.P50);
            Assert.AreEqual(5, stats.P10);
        }
    }
}
