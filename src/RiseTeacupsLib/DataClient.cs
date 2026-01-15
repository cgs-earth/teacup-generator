using Newtonsoft.Json;
using System.Net;
using System.Web;

namespace RiseTeacupsLib
{
    public class DataClient
    {
        public Dictionary<DateTime, double> Data { get; set; }
        private HttpClient _httpClient { get; set; }
        private Location _location { get; set; }
        private static HttpResponseMessage _latestResponse { get; set; }

        private static readonly int _maxRetries = 2;

        public DataClient(Location l, HttpClient httpClient)
        {
            // gets instatiated via Location.FromCsv
            Data = new Dictionary<DateTime, double>();
            _location = l;
            _httpClient = httpClient;
        }
   

        public void FetchData(DateTime start, DateTime end, bool includeRise, bool useTest)
        {
            try
            {
                if (_location.RiseLocationId != null && includeRise)
                {
                    int pageNum = 1;
                    int maxPageNum = 1;
                    int itemsPerPage = 10000; // this is the max that the api allows
                    string urlRoot = useTest 
                        ? "http://140.215.104.206/rise-api/index.php/api/result"
                        : "https://data.usbr.gov/rise/api/result";

                    while (pageNum <= maxPageNum)
                    {
                        Console.WriteLine(maxPageNum != 1 ? $"Page {pageNum} of {maxPageNum}" : $"Page {pageNum}");
                        string url = urlRoot + $"?page={pageNum}&itemsPerPage={itemsPerPage}&locationId={_location.RiseLocationId}&parameterId=3&dateTime[before]={end.ToString("yyyy-MM-dd")}&dateTime[after]={start.ToString("yyyy-MM-dd")}&catalogItem.isModeled=false&order[dateTime]=desc";
                        RiseResult result = DecorateWithRetry<RiseResult>(GetRiseData, url);
                        pageNum++;
                        if (result == null)
                        {
                            // if no data found in RISE, use other sources
                            Console.WriteLine("Falling back to external data sources");
                            FetchData(start, end, false, useTest);
                            return;
                            //continue;
                        }
                        if (maxPageNum == 1 && result?.Links?.Last != null)
                        {
                            Uri lastLink = new Uri(urlRoot + result.Links.Last);
                            maxPageNum = Convert.ToInt16(HttpUtility.ParseQueryString(lastLink.Query).Get("page"));
                        }
                    }
                }
                else
                {
                    switch (_location.DataSource)
                    {
                        case "usgs":
                            //GetUsgsData(_location.DataIdentifier, start, end);
                            DecorateWithRetry<object>(GetUsgsData, _location.DataIdentifier, start, end);
                            break;
                        case "pnhyd":
                            //GetHydrometData("PN", _location.DataIdentifier, start, end);
                            DecorateWithRetry<object>(GetHydrometData, "PN", _location.DataIdentifier, start, end);
                            break;
                        case "gphyd":
                            //GetHydrometData("GP", _location.DataIdentifier, start, end);
                            DecorateWithRetry<object>(GetHydrometData, "GP", _location.DataIdentifier, start, end);
                            break;
                        case "cdec":
                            //GetCdecData(_location.DataIdentifier, start, end);
                            DecorateWithRetry<object>(GetCdecData, _location.DataIdentifier, start, end);
                            break;
                        case "uchdb2":
                            //GetHdbApiData("uchdb2", _location.DataIdentifier, start, end);
                            DecorateWithRetry<object>(GetHdbApiData, "uchdb2", _location.DataIdentifier, start, end);
                            break;
                        case "lchdb":
                            //GetHdbApiData("lchdb", _location.DataIdentifier, start, end);
                            DecorateWithRetry<object>(GetHdbApiData, "lchdb", _location.DataIdentifier, start, end);
                            break;
                        default:
                            break;
                    }
                }
                Console.WriteLine(Data.Count > 0 ? $"--- Success, {Data.Count} records found" : "--- 0 records found");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"--- Failed: {ex} \n");
            }
            // _httpClient.Dispose();
        }

        public T? DecorateWithRetry<T>(Delegate method, params object[] parameters)
        {
            int retryCount = 0;
            while (retryCount <= _maxRetries)
            {
                try
                {
                    if (typeof(T) != typeof(object))
                    {
                        // return the results if T was specified
                        return (method.DynamicInvoke(parameters) as Task<T>).GetAwaiter().GetResult();
                    }

                    (method.DynamicInvoke(parameters) as Task).Wait();
                    return default(T);
                }
                catch (Exception e)
                {
                    if (retryCount != _maxRetries)
                    {
                        if (e is TaskCanceledException)
                        {
                            Console.WriteLine("Request exceeded timeout limit");
                        }
                        Console.WriteLine("Retrying...");
                        Thread.Sleep(5000 * (retryCount + 1));
                    }
                    else
                    {
                        LogHttpResponse(_latestResponse);
                        string errorString = e.ToString();
                        if (errorString.Length > 500)
                        {
                            errorString = errorString.Substring(0, 500);
                        }
                        Console.WriteLine($"\n{errorString}...\n");
                    }
                    retryCount++;
                }
            }

            return default(T); // null
        }

        private async Task<RiseResult> GetRiseData(string url)
        {
            HttpResponseMessage response = await _httpClient.GetAsync(url);
            _latestResponse = response;

            string json = await response.Content.ReadAsStringAsync();

            RiseResult result = JsonConvert.DeserializeObject<RiseResult>(json);

            foreach (Data d in result.Data)
            {
                if (!Data.ContainsKey(d.Attributes.DateTime.Date))
                {
                    Data.Add(d.Attributes.DateTime.Date, Convert.ToDouble(d?.Attributes?.Result));
                }
            }

            if (Data.Count == 0)
            {
                // trigger a retry if no data was found
                throw new Exception("No data found");
            }

            //response.Dispose();
            return result;
        }

        private async Task GetHydrometData(string region, string sdid, DateTime startDate, DateTime endDate)
        {
            string url = @"https://www.usbr.gov/pn-bin/daily.pl?parameter=$SDI$&syer=$Y1$&smnth=$M1$&sdy=$D1$&eyer=$Y2$&emnth=$M2$&edy=$D2$&format=csv";
            if (region == "GP")
            {
                url = @"https://www.usbr.gov/gp-bin/webarccsv.pl?parameter=$SDI$&syer=$Y1$&smnth=$M1$&sdy=$D1$&eyer=$Y2$&emnth=$M2$&edy=$D2$&format=4";
            }
            url = url.Replace("$SDI$", sdid);
            url = url.Replace("$Y1$", startDate.Year.ToString("F0"));
            url = url.Replace("$Y2$", endDate.Year.ToString("F0"));
            url = url.Replace("$M1$", startDate.Month.ToString("F0"));
            url = url.Replace("$M2$", endDate.Month.ToString("F0"));
            url = url.Replace("$D1$", startDate.Day.ToString("F0"));
            url = url.Replace("$D2$", endDate.Day.ToString("F0"));

            HttpResponseMessage response = await _httpClient.GetAsync(url);
            _latestResponse = response;

            Stream stream = await response.Content.ReadAsStreamAsync();
            using (StreamReader sr = new StreamReader(stream))
            {
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    if (line.ToLower().Contains("error"))
                    {
                        throw new Exception("Unable to get data from Hydromet");
                    }

                    try
                    {
                        var values = line.Split(',');
                        Data.Add(DateTime.Parse(values[0]), Convert.ToDouble(values[1]));
                    }
                    catch
                    {
                        // skip
                    }
                }
            }

            //response.Dispose();
            if (Data.Count == 0)
            {
                // trigger a retry if no data was found
                throw new Exception("No data found");
            }
        }

        private async Task GetUsgsData(string siteNum, DateTime startDate, DateTime endDate)
        {
            string url = @"https://nwis.waterdata.usgs.gov/usa/nwis/dv/?cb_72275=on&format=rdb&site_no=$SITENUM$&period=&begin_date=$T1$&end_date=$T2$";
            url = url.Replace("$SITENUM$", siteNum);
            url = url.Replace("$T1$", startDate.ToString("yyyy-MM-dd"));
            url = url.Replace("$T2$", endDate.ToString("yyyy-MM-dd"));

            HttpResponseMessage response = await _httpClient.GetAsync(url);
            _latestResponse = response;

            Stream stream = await response.Content.ReadAsStreamAsync();
            using (StreamReader sr = new StreamReader(stream))
            {
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    try
                    {
                        var values = line.Split(new char[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                        if (values[0] == "USGS" && values[3] != null)
                        {
                            Data.Add(
                                DateTime.Parse(values[2]),
                                GetKlamathStorage(Convert.ToDouble(values[3]))
                            );
                        }
                    }
                    catch
                    {
                        // skip
                    }
                }
            }

            //response.Dispose();
            if (Data.Count == 0)
            {
                // trigger a retry if no data was found
                throw new Exception("No data found");
            }
        }

        private async Task GetCdecData(string station, DateTime startDate, DateTime endDate)
        {
            string url = @"https://cdec.water.ca.gov/dynamicapp/req/CSVDataServlet?Stations=$STATION$&SensorNums=15&dur_code=D&Start=$T1$&End=$T2$";
            url = url.Replace("$STATION$", station);
            url = url.Replace("$T1$", startDate.ToString("yyyy-MM-dd"));
            url = url.Replace("$T2$", endDate.ToString("yyyy-MM-dd"));

            HttpResponseMessage response = await _httpClient.GetAsync(url);
            _latestResponse = response;

            Stream stream = await response.Content.ReadAsStreamAsync();
            using (StreamReader sr = new StreamReader(stream))
            {
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    try
                    {
                        var values = line.Split(',');
                        if (values[0] == station.ToUpper())
                        {
                            DateTime dataDate = DateTime.ParseExact(values[4], "yyyyMMdd HHmm", System.Globalization.CultureInfo.InvariantCulture);
                            Data.Add(dataDate, Convert.ToDouble(values[6]));
                        }
                    }
                    catch
                    {
                        // skip
                    }
                }
            }

            //response.Dispose();
            if (Data.Count == 0)
            {
                // trigger a retry if no data was found
                throw new Exception("No data found");
            }
        }

        private async Task GetHdbApiData(string db, string sdid, DateTime startDate, DateTime endDate)
        {
            string url = @"https://www.usbr.gov/pn-bin/hdb/hdb.pl?svr=$DB$&sdi=$SDI$&tstp=DY&t1=$T1$&t2=$T2$&format=88";
            url = url.Replace("$DB$", db);
            url = url.Replace("$SDI$", sdid);
            url = url.Replace("$T1$", startDate.ToString("yyyy-MM-dd"));
            url = url.Replace("$T2$", endDate.ToString("yyyy-MM-dd"));

            HttpResponseMessage response = await _httpClient.GetAsync(url);
            _latestResponse = response;

            Stream stream = await response.Content.ReadAsStreamAsync();
            using (StreamReader sr = new StreamReader(stream))
            {
                while (!sr.EndOfStream)
                {
                    string line = sr.ReadLine();
                    if (line.ToLower().Contains("error"))
                    {
                        throw new Exception("Unable to get data from HDB");
                    }

                    try
                    {
                        var values = line.Split(',');
                        Data.Add(DateTime.Parse(values[0]), Convert.ToDouble(values[1]));
                    }
                    catch
                    {
                        //skip;
                    }
                }
            }

            //response.Dispose();
            if (Data.Count == 0)
            {
                // trigger a retry if no data was found
                throw new Exception("No data found");
            }
        }

        public KeyValuePair<DateTime, string> GetMostRecentValue(DateTime endDate)
        {
            DateTime minDate = endDate.AddDays(-7);
            DateTime dataDate = endDate;
            string dataValue = "NaN";

            for (DateTime t = endDate; t >= minDate; t = t.AddDays(-1))
            {
                if (dataValue != "NaN")
                {
                    break;
                }
                try
                {
                    dataValue = Data[t.Date].ToString();
                    dataDate = t;
                }
                catch
                {
                    if (t.Equals(minDate))
                    {
                        Console.WriteLine("No data found");
                    }
                }
            }
            return new KeyValuePair<DateTime, string>(dataDate, dataValue);
        }

        public Dictionary<DateTime, double> ReadHistoricalData()
        {
            using (StreamReader sr = new StreamReader($"historicalData/{_location.SiteName}.json"))
            {
                string json = sr.ReadToEnd();
                var data = JsonConvert.DeserializeObject<Dictionary<DateTime, double>>(json);
                return data;
            }
        }

        public Stats CalculateSummaryStatistics()
        {
            Stats stats = new Stats();
            Dictionary<DateTime, double> historicalData = ReadHistoricalData();
            List<double> values = historicalData.Keys
                .Where(k => k.Month == _location.RunDate.Month && k.Day == _location.RunDate.Day)
                .Select(k => historicalData[k])
                .ToList();
            values.Sort();
            stats.Max = values.Max();
            stats.P90 = CalculatePercentile(90, values);
            stats.P75 = CalculatePercentile(75, values);
            stats.P50 = CalculatePercentile(50, values);
            stats.P25 = CalculatePercentile(25, values);
            stats.P10 = CalculatePercentile(10, values);
            stats.Avg = values.Average();
            stats.Min = values.Min();
            return stats;
        }

        private double CalculatePercentile(double percentile, List<double> values)
        {
            double index = (percentile / 100) * values.Count;
            double indexRounded = Math.Round(index, 0, MidpointRounding.AwayFromZero);
            double result = values[Convert.ToInt32(indexRounded - 1)];
            return result;
        }

        public static double GetKlamathStorage(double elev)
        {
            // make into a function?
            double[] uklElevs = [4136.00, 4136.01, 4136.02, 4136.03, 4136.04, 4136.05, 4136.06, 4136.07, 4136.08, 4136.09, 4136.10, 4136.11, 4136.12, 4136.13, 4136.14, 4136.15, 4136.16, 4136.17, 4136.18, 4136.19, 4136.20, 4136.21, 4136.22, 4136.23, 4136.24, 4136.25, 4136.26, 4136.27, 4136.28, 4136.29, 4136.30, 4136.31, 4136.32, 4136.33, 4136.34, 4136.35, 4136.36, 4136.37, 4136.38, 4136.39, 4136.40, 4136.41, 4136.42, 4136.43, 4136.44, 4136.45, 4136.46, 4136.47, 4136.48, 4136.49, 4136.50, 4136.51, 4136.52, 4136.53, 4136.54, 4136.55, 4136.56, 4136.57, 4136.58, 4136.59, 4136.60, 4136.61, 4136.62, 4136.63, 4136.64, 4136.65, 4136.66, 4136.67, 4136.68, 4136.69, 4136.70, 4136.71, 4136.72, 4136.73, 4136.74, 4136.75, 4136.76, 4136.77, 4136.78, 4136.79, 4136.80, 4136.81, 4136.82, 4136.83, 4136.84, 4136.85, 4136.86, 4136.87, 4136.88, 4136.89, 4136.90, 4136.91, 4136.92, 4136.93, 4136.94, 4136.95, 4136.96, 4136.97, 4136.98, 4136.99, 4137.00, 4137.01, 4137.02, 4137.03, 4137.04, 4137.05, 4137.06, 4137.07, 4137.08, 4137.09, 4137.10, 4137.11, 4137.12, 4137.13, 4137.14, 4137.15, 4137.16, 4137.17, 4137.18, 4137.19, 4137.20, 4137.21, 4137.22, 4137.23, 4137.24, 4137.25, 4137.26, 4137.27, 4137.28, 4137.29, 4137.30, 4137.31, 4137.32, 4137.33, 4137.34, 4137.35, 4137.36, 4137.37, 4137.38, 4137.39, 4137.40, 4137.41, 4137.42, 4137.43, 4137.44, 4137.45, 4137.46, 4137.47, 4137.48, 4137.49, 4137.50, 4137.51, 4137.52, 4137.53, 4137.54, 4137.55, 4137.56, 4137.57, 4137.58, 4137.59, 4137.60, 4137.61, 4137.62, 4137.63, 4137.64, 4137.65, 4137.66, 4137.67, 4137.68, 4137.69, 4137.70, 4137.71, 4137.72, 4137.73, 4137.74, 4137.75, 4137.76, 4137.77, 4137.78, 4137.79, 4137.80, 4137.81, 4137.82, 4137.83, 4137.84, 4137.85, 4137.86, 4137.87, 4137.88, 4137.89, 4137.90, 4137.91, 4137.92, 4137.93, 4137.94, 4137.95, 4137.96, 4137.97, 4137.98, 4137.99, 4138.00, 4138.01, 4138.02, 4138.03, 4138.04, 4138.05, 4138.06, 4138.07, 4138.08, 4138.09, 4138.10, 4138.11, 4138.12, 4138.13, 4138.14, 4138.15, 4138.16, 4138.17, 4138.18, 4138.19, 4138.20, 4138.21, 4138.22, 4138.23, 4138.24, 4138.25, 4138.26, 4138.27, 4138.28, 4138.29, 4138.30, 4138.31, 4138.32, 4138.33, 4138.34, 4138.35, 4138.36, 4138.37, 4138.38, 4138.39, 4138.40, 4138.41, 4138.42, 4138.43, 4138.44, 4138.45, 4138.46, 4138.47, 4138.48, 4138.49, 4138.50, 4138.51, 4138.52, 4138.53, 4138.54, 4138.55, 4138.56, 4138.57, 4138.58, 4138.59, 4138.60, 4138.61, 4138.62, 4138.63, 4138.64, 4138.65, 4138.66, 4138.67, 4138.68, 4138.69, 4138.70, 4138.71, 4138.72, 4138.73, 4138.74, 4138.75, 4138.76, 4138.77, 4138.78, 4138.79, 4138.80, 4138.81, 4138.82, 4138.83, 4138.84, 4138.85, 4138.86, 4138.87, 4138.88, 4138.89, 4138.90, 4138.91, 4138.92, 4138.93, 4138.94, 4138.95, 4138.96, 4138.97, 4138.98, 4138.99, 4139.00, 4139.01, 4139.02, 4139.03, 4139.04, 4139.05, 4139.06, 4139.07, 4139.08, 4139.09, 4139.10, 4139.11, 4139.12, 4139.13, 4139.14, 4139.15, 4139.16, 4139.17, 4139.18, 4139.19, 4139.20, 4139.21, 4139.22, 4139.23, 4139.24, 4139.25, 4139.26, 4139.27, 4139.28, 4139.29, 4139.30, 4139.31, 4139.32, 4139.33, 4139.34, 4139.35, 4139.36, 4139.37, 4139.38, 4139.39, 4139.40, 4139.41, 4139.42, 4139.43, 4139.44, 4139.45, 4139.46, 4139.47, 4139.48, 4139.49, 4139.50, 4139.51, 4139.52, 4139.53, 4139.54, 4139.55, 4139.56, 4139.57, 4139.58, 4139.59, 4139.60, 4139.61, 4139.62, 4139.63, 4139.64, 4139.65, 4139.66, 4139.67, 4139.68, 4139.69, 4139.70, 4139.71, 4139.72, 4139.73, 4139.74, 4139.75, 4139.76, 4139.77, 4139.78, 4139.79, 4139.80, 4139.81, 4139.82, 4139.83, 4139.84, 4139.85, 4139.86, 4139.87, 4139.88, 4139.89, 4139.90, 4139.91, 4139.92, 4139.93, 4139.94, 4139.95, 4139.96, 4139.97, 4139.98, 4139.99, 4140.00, 4140.01, 4140.02, 4140.03, 4140.04, 4140.05, 4140.06, 4140.07, 4140.08, 4140.09, 4140.10, 4140.11, 4140.12, 4140.13, 4140.14, 4140.15, 4140.16, 4140.17, 4140.18, 4140.19, 4140.20, 4140.21, 4140.22, 4140.23, 4140.24, 4140.25, 4140.26, 4140.27, 4140.28, 4140.29, 4140.30, 4140.31, 4140.32, 4140.33, 4140.34, 4140.35, 4140.36, 4140.37, 4140.38, 4140.39, 4140.40, 4140.41, 4140.42, 4140.43, 4140.44, 4140.45, 4140.46, 4140.47, 4140.48, 4140.49, 4140.50, 4140.51, 4140.52, 4140.53, 4140.54, 4140.55, 4140.56, 4140.57, 4140.58, 4140.59, 4140.60, 4140.61, 4140.62, 4140.63, 4140.64, 4140.65, 4140.66, 4140.67, 4140.68, 4140.69, 4140.70, 4140.71, 4140.72, 4140.73, 4140.74, 4140.75, 4140.76, 4140.77, 4140.78, 4140.79, 4140.80, 4140.81, 4140.82, 4140.83, 4140.84, 4140.85, 4140.86, 4140.87, 4140.88, 4140.89, 4140.90, 4140.91, 4140.92, 4140.93, 4140.94, 4140.95, 4140.96, 4140.97, 4140.98, 4140.99, 4141.00, 4141.01, 4141.02, 4141.03, 4141.04, 4141.05, 4141.06, 4141.07, 4141.08, 4141.09, 4141.10, 4141.11, 4141.12, 4141.13, 4141.14, 4141.15, 4141.16, 4141.17, 4141.18, 4141.19, 4141.20, 4141.21, 4141.22, 4141.23, 4141.24, 4141.25, 4141.26, 4141.27, 4141.28, 4141.29, 4141.30, 4141.31, 4141.32, 4141.33, 4141.34, 4141.35, 4141.36, 4141.37, 4141.38, 4141.39, 4141.40, 4141.41, 4141.42, 4141.43, 4141.44, 4141.45, 4141.46, 4141.47, 4141.48, 4141.49, 4141.50, 4141.51, 4141.52, 4141.53, 4141.54, 4141.55, 4141.56, 4141.57, 4141.58, 4141.59, 4141.60, 4141.61, 4141.62, 4141.63, 4141.64, 4141.65, 4141.66, 4141.67, 4141.68, 4141.69, 4141.70, 4141.71, 4141.72, 4141.73, 4141.74, 4141.75, 4141.76, 4141.77, 4141.78, 4141.79, 4141.80, 4141.81, 4141.82, 4141.83, 4141.84, 4141.85, 4141.86, 4141.87, 4141.88, 4141.89, 4141.90, 4141.91, 4141.92, 4141.93, 4141.94, 4141.95, 4141.96, 4141.97, 4141.98, 4141.99, 4142.00, 4142.01, 4142.02, 4142.03, 4142.04, 4142.05, 4142.06, 4142.07, 4142.08, 4142.09, 4142.10, 4142.11, 4142.12, 4142.13, 4142.14, 4142.15, 4142.16, 4142.17, 4142.18, 4142.19, 4142.20, 4142.21, 4142.22, 4142.23, 4142.24, 4142.25, 4142.26, 4142.27, 4142.28, 4142.29, 4142.30, 4142.31, 4142.32, 4142.33, 4142.34, 4142.35, 4142.36, 4142.37, 4142.38, 4142.39, 4142.40, 4142.41, 4142.42, 4142.43, 4142.44, 4142.45, 4142.46, 4142.47, 4142.48, 4142.49, 4142.50, 4142.51, 4142.52, 4142.53, 4142.54, 4142.55, 4142.56, 4142.57, 4142.58, 4142.59, 4142.60, 4142.61, 4142.62, 4142.63, 4142.64, 4142.65, 4142.66, 4142.67, 4142.68, 4142.69, 4142.70, 4142.71, 4142.72, 4142.73, 4142.74, 4142.75, 4142.76, 4142.77, 4142.78, 4142.79, 4142.80, 4142.81, 4142.82, 4142.83, 4142.84, 4142.85, 4142.86, 4142.87, 4142.88, 4142.89, 4142.90, 4142.91, 4142.92, 4142.93, 4142.94, 4142.95, 4142.96, 4142.97, 4142.98, 4142.99, 4143.00, 4143.01, 4143.02, 4143.03, 4143.04, 4143.05, 4143.06, 4143.07, 4143.08, 4143.09, 4143.10, 4143.11, 4143.12, 4143.13, 4143.14, 4143.15, 4143.16, 4143.17, 4143.18, 4143.19, 4143.20, 4143.21, 4143.22, 4143.23, 4143.24, 4143.25, 4143.26, 4143.27, 4143.28, 4143.29, 4143.30, 4143.31, 4143.32, 4143.33, 4143.34, 4143.35, 4143.36, 4143.37, 4143.38, 4143.39, 4143.40, 4143.41, 4143.42, 4143.43, 4143.44, 4143.45, 4143.46, 4143.47, 4143.48, 4143.49, 4143.50, 4150.00];
            double[] uklStors = [0.0, 659.00, 1318.00, 1977.00, 2636.00, 3294.00, 3953.00, 4617.00, 5282.00, 5946.00, 6610.00, 7274.00, 7938.00, 8602.00, 9266.00, 9930.00, 10594.00, 11259.00, 11925.00, 12590.00, 13255.00, 13921.00, 14586.00, 15252.00, 15917.00, 16582.00, 17248.00, 17914.00, 18581.00, 19248.00, 19914.00, 20581.00, 21247.00, 21914.00, 22580.00, 23247.00, 23913.00, 24581.00, 25249.00, 25917.00, 26585.00, 27252.00, 27920.00, 28588.00, 29256.00, 29924.00, 30591.00, 31257.00, 31923.00, 32589.00, 33255.00, 33920.00, 34586.00, 35252.00, 35918.00, 36584.00, 37249.00, 37919.00, 38589.00, 39259.00, 39929.00, 40599.00, 41269.00, 41939.00, 42610.00, 43280.00, 43950.00, 44620.00, 45291.00, 45962.00, 46633.00, 47304.00, 47974.00, 48645.00, 49316.00, 49987.00, 50658.00, 51329.00, 52001.00, 52672.00, 53344.00, 54015.00, 54687.00, 55358.00, 56030.00, 56701.00, 57373.00, 58045.00, 58718.00, 59390.00, 60062.00, 60734.00, 61407.00, 62079.00, 62751.00, 63424.00, 64096.00, 64766.00, 65435.00, 66105.00, 66775.00, 67444.00, 68114.00, 68784.00, 69454.00, 70123.00, 70793.00, 71467.00, 72140.00, 72814.00, 73488.00, 74162.00, 74835.00, 75509.00, 76183.00, 76857.00, 77530.00, 78205.00, 78879.00, 79554.00, 80228.00, 80902.00, 81577.00, 82251.00, 82926.00, 83600.00, 84275.00, 84950.00, 85625.00, 86300.00, 86975.00, 87651.00, 88326.00, 89001.00, 89676.00, 90351.00, 91026.00, 91702.00, 92378.00, 93054.00, 93730.00, 94406.00, 95082.00, 95757.00, 96433.00, 97109.00, 97785.00, 98458.00, 99132.00, 99805.00, 100478.00, 101151.00, 101825.00, 102498.00, 103171.00, 103844.00, 104518.00, 105195.00, 105872.00, 106550.00, 107227.00, 107904.00, 108581.00, 109259.00, 109936.00, 110613.00, 111290.00, 111968.00, 112646.00, 113324.00, 114002.00, 114680.00, 115358.00, 116036.00, 116714.00, 117392.00, 118070.00, 118749.00, 119428.00, 120106.00, 120785.00, 121464.00, 122143.00, 122821.00, 123500.00, 124179.00, 124857.00, 125537.00, 126216.00, 126896.00, 127576.00, 128255.00, 128935.00, 129614.00, 130294.00, 130973.00, 131653.00, 132331.00, 133010.00, 133688.00, 134367.00, 135045.00, 135724.00, 136402.00, 137081.00, 137760.00, 138438.00, 139122.00, 139807.00, 140491.00, 141175.00, 141860.00, 142544.00, 143228.00, 143913.00, 144597.00, 145281.00, 145967.00, 146652.00, 147338.00, 148023.00, 148709.00, 149394.00, 150079.00, 150765.00, 151450.00, 152136.00, 152822.00, 153509.00, 154195.00, 154882.00, 155568.00, 156255.00, 156941.00, 157628.00, 158314.00, 159001.00, 159688.00, 160376.00, 161063.00, 161751.00, 162438.00, 163126.00, 163813.00, 164501.00, 165188.00, 165876.00, 166561.00, 167246.00, 167931.00, 168616.00, 169302.00, 169987.00, 170672.00, 171357.00, 172042.00, 172727.00, 173417.00, 174107.00, 174796.00, 175486.00, 176175.00, 176865.00, 177554.00, 178244.00, 178933.00, 179623.00, 180313.00, 181004.00, 181695.00, 182385.00, 183076.00, 183766.00, 184457.00, 185147.00, 185838.00, 186529.00, 187220.00, 187912.00, 188603.00, 189295.00, 189987.00, 190678.00, 191370.00, 192061.00, 192753.00, 193445.00, 194137.00, 194830.00, 195523.00, 196215.00, 196908.00, 197601.00, 198293.00, 198986.00, 199679.00, 200371.00, 201061.00, 201752.00, 202442.00, 203133.00, 203823.00, 204513.00, 205204.00, 205894.00, 206584.00, 207275.00, 207969.00, 208664.00, 209359.00, 210054.00, 210749.00, 211443.00, 212138.00, 212833.00, 213528.00, 214223.00, 214919.00, 215615.00, 216311.00, 217007.00, 217703.00, 218398.00, 219094.00, 219790.00, 220486.00, 221182.00, 221879.00, 222577.00, 223274.00, 223971.00, 224668.00, 225365.00, 226062.00, 226759.00, 227457.00, 228154.00, 228865.00, 229576.00, 230287.00, 230998.00, 231709.00, 232420.00, 233131.00, 233843.00, 234554.00, 235265.00, 235994.00, 236723.00, 237452.00, 238182.00, 238911.00, 239640.00, 240369.00, 241099.00, 241828.00, 242557.00, 243299.00, 244041.00, 244783.00, 245526.00, 246268.00, 247010.00, 247752.00, 248494.00, 249236.00, 249979.00, 250727.00, 251475.00, 252223.00, 252971.00, 253719.00, 254467.00, 255215.00, 255963.00, 256711.00, 257459.00, 258212.00, 258965.00, 259718.00, 260471.00, 261224.00, 261977.00, 262730.00, 263483.00, 264236.00, 264989.00, 265747.00, 266505.00, 267263.00, 268021.00, 268779.00, 269537.00, 270294.00, 271052.00, 271810.00, 272568.00, 273328.00, 274088.00, 274848.00, 275608.00, 276368.00, 277128.00, 277888.00, 278648.00, 279408.00, 280168.00, 280938.00, 281707.00, 282477.00, 283247.00, 284016.00, 284786.00, 285556.00, 286325.00, 287095.00, 287865.00, 288641.00, 289417.00, 290193.00, 290968.00, 291744.00, 292520.00, 293296.00, 294072.00, 294848.00, 295624.00, 296407.00, 297190.00, 297973.00, 298756.00, 299539.00, 300322.00, 301105.00, 301888.00, 302671.00, 303454.00, 304245.00, 305036.00, 305827.00, 306618.00, 307410.00, 308201.00, 308992.00, 309783.00, 310574.00, 311366.00, 312162.00, 312959.00, 313755.00, 314552.00, 315348.00, 316145.00, 316941.00, 317738.00, 318534.00, 319331.00, 320142.00, 320952.00, 321763.00, 322574.00, 323385.00, 324195.00, 325006.00, 325817.00, 326627.00, 327438.00, 328260.00, 329081.00, 329903.00, 330725.00, 331547.00, 332368.00, 333190.00, 334012.00, 334834.00, 335655.00, 336488.00, 337321.00, 338154.00, 338987.00, 339820.00, 340653.00, 341486.00, 342319.00, 343152.00, 343985.00, 344828.00, 345672.00, 346516.00, 347359.00, 348203.00, 349047.00, 349890.00, 350734.00, 351578.00, 352421.00, 353271.00, 354120.00, 354969.00, 355819.00, 356668.00, 357518.00, 358367.00, 359216.00, 360066.00, 360915.00, 361777.00, 362640.00, 363502.00, 364364.00, 365227.00, 366089.00, 366951.00, 367813.00, 368676.00, 369538.00, 370408.00, 371278.00, 372148.00, 373018.00, 373888.00, 374758.00, 375628.00, 376498.00, 377369.00, 378239.00, 379115.00, 379992.00, 380869.00, 381746.00, 382622.00, 383499.00, 384376.00, 385253.00, 386129.00, 387006.00, 387888.00, 388771.00, 389653.00, 390535.00, 391418.00, 392300.00, 393182.00, 394065.00, 394947.00, 395829.00, 396712.00, 397594.00, 398477.00, 399359.00, 400242.00, 401124.00, 402007.00, 402889.00, 403772.00, 404654.00, 405545.00, 406435.00, 407326.00, 408216.00, 409107.00, 409997.00, 410888.00, 411778.00, 412669.00, 413559.00, 414452.00, 415346.00, 416239.00, 417133.00, 418026.00, 418920.00, 419813.00, 420707.00, 421600.00, 422494.00, 423390.00, 424286.00, 425182.00, 426078.00, 426974.00, 427870.00, 428766.00, 429662.00, 430559.00, 431455.00, 432353.00, 433251.00, 434149.00, 435047.00, 435945.00, 436843.00, 437742.00, 438640.00, 439538.00, 440436.00, 441331.00, 442227.00, 443122.00, 444018.00, 444913.00, 445809.00, 446704.00, 447600.00, 448495.00, 449391.00, 450292.00, 451193.00, 452095.00, 452996.00, 453897.00, 454799.00, 455700.00, 456601.00, 457503.00, 458404.00, 459307.00, 460209.00, 461112.00, 462015.00, 462917.00, 463820.00, 464723.00, 465625.00, 466528.00, 467431.00, 468335.00, 469239.00, 470143.00, 471047.00, 471950.00, 472854.00, 473758.00, 474662.00, 475566.00, 476470.00, 477375.00, 478280.00, 479185.00, 480090.00, 480995.00, 481900.00, 482805.00, 483711.00, 484616.00, 485521.00, 486422.00, 487324.00, 488226.00, 489127.00, 490029.00, 490931.00, 491832.00, 492734.00, 493636.00, 494537.00, 495444.00, 496351.00, 497258.00, 498165.00, 499072.00, 499979.00, 500887.00, 501794.00, 502701.00, 503608.00, 504516.00, 505424.00, 506332.00, 507240.00, 508148.00, 509056.00, 509964.00, 510872.00, 511780.00, 512688.00, 513597.00, 514506.00, 515415.00, 516324.00, 517233.00, 518142.00, 519051.00, 519960.00, 520869.00, 521777.00, 522687.00, 523597.00, 524507.00, 525417.00, 526327.00, 527237.00, 528147.00, 529057.00, 529966.00, 530876.00, 531783.00, 532689.00, 533595.00, 534502.00, 535408.00, 536314.00, 537221.00, 538127.00, 539034.00, 539940.00, 540852.00, 541763.00, 542675.00, 543587.00, 544498.00, 545410.00, 546322.00, 547233.00, 548145.00, 549057.00, 549969.00, 550882.00, 551795.00, 552707.00, 553620.00, 554533.00, 555445.00, 556358.00, 557270.00, 558183.00, 559097.00, 560010.00, 560924.00, 561838.00, 562751.00, 563665.00, 564579.00, 565492.00, 566406.00, 567319.00, 568233.00, 569147.00, 570060.00, 570974.00, 571888.00, 572801.00, 573715.00, 574629.00, 575542.00, 576456.00, 577369.00, 578283.00, 579197.00, 580110.00, 580110.00];

            double storageVal;
            if (elev <= 4136.0)
            {
                return 0.0;
            }
            if (elev > 4150.0)
            {
                return 580110.00;
            }
            double[] elevs = uklElevs;
            double[] stors = uklStors;

            elev = System.Math.Round(elev, 2);
            storageVal = stors[Array.IndexOf(uklElevs, elev)];
            return storageVal;
        }

        //public string GetHttpResponse(string url)
        //{
        //    HttpWebRequest req = (HttpWebRequest)WebRequest.Create(url);
        //    HttpWebResponse resp = (HttpWebResponse)req.GetResponse();
        //    StreamReader sr = new StreamReader(resp.GetResponseStream());
        //    string results = sr.ReadToEnd();
        //    sr.Close();
        //    resp.Close();
        //    return results;
        //}

        public async void LogHttpResponse(HttpResponseMessage response)
        {
            if (response.Headers.Date != null)
            {
                Console.WriteLine($"Request Timestamp: {response.Headers.Date.Value.DateTime.ToLocalTime()}");
            }
            Console.WriteLine($"Status: {response.StatusCode}");
            string json = await response.Content.ReadAsStringAsync();
            Console.WriteLine(json.Length > 500 ? $"Content: {json.Substring(0, 500)}..." : $"Content: {json}");
        }

        public void LogDataToConsole(string siteName)
        {
            if (_location.SiteName == siteName)
            {
                Console.WriteLine($"Logging data for {_location.SiteName}");
                foreach (KeyValuePair<DateTime, double> kvp in Data)
                {
                    Console.WriteLine($"{kvp.Key} - {kvp.Value}");
                }
            }
        }

        public void LogDataToConsole()
        {
            Console.WriteLine($"Logging data for {_location.SiteName}");
            foreach (KeyValuePair<DateTime, double> kvp in Data)
            {
                Console.WriteLine($"{kvp.Key} - {kvp.Value}");
            }
        }
    }
}
