using Microsoft.VisualBasic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Net;

namespace RiseTeacupsLib
{
    public class Location
    {
        public string DataSource { get; set; }
        public string DataIdentifier { get; set; }
        public string SiteName { get; set; }
        public float Lat { get; set; }
        public float Long { get; set; }
        public string Units { get; set; }
        public DateTime MinDate { get; set; }
        public string State { get; set; }
        public string DoiRegion { get; set; }
        public int Huc8 { get; set; }
        public int MaxCapacity { get; set; }
        public int StatsPeriod { get; set; }
        public string TeacupName { get; set; }
        public string? DataUrl { get; set; }
        public string? Comment { get; set; }
        public int? RiseLocationId { get; set; }
        public DataClient? DataClient { get; set; }
        public Stats? Stats { get; set; }
        public DateTime RunDate { get; set; }

        public static Location FromCsv(string csvLine, DateTime runDate)
        {
            // expects csvLine to be a line from dataDownload.csv
            string[] values = csvLine.Split(',');
            Location location = new Location();
            location.DataSource = values[0];
            location.DataIdentifier = values[1];
            location.SiteName = values[2];
            location.Lat = float.Parse(values[3]);
            location.Long = float.Parse(values[4]);
            location.Units = values[5];
            location.MinDate = DateTime.Parse(values[6]);
            location.State = values[7];
            location.DoiRegion = values[8];
            location.Huc8 = int.Parse(values[9]);
            location.MaxCapacity = int.Parse(values[10]);
            location.StatsPeriod = int.Parse(values[11]);
            location.TeacupName = values[12];
            location.DataUrl = values[13];
            location.Comment = values[14];
            location.RiseLocationId = values[15] != String.Empty ? int.Parse(values[15]) : null;
            location.DataClient = new DataClient(location, new HttpClient()
            {
                Timeout = TimeSpan.FromSeconds(300)
            }); // instantiate data client here
            location.RunDate = runDate;
            return location;
        }

        public string GenerateCsvLine()
        {
            KeyValuePair<DateTime, string> dataValue = DataClient.GetMostRecentValue(RunDate.Date);
            string outLine = "";
            outLine += $"{SiteName}, ";
            outLine += $"{Lat}, ";
            outLine += $"{Long}, ";
            outLine += $"{State}, ";
            outLine += $"{DoiRegion}, ";
            outLine += $"{Huc8}, ";
            outLine += $"{Units}, ";
            outLine += $"{dataValue.Value}, ";
            outLine += $"{dataValue.Key.Date.ToString("MM/dd/yyyy")}, ";
            outLine += $"{DateTime.Now.Date.ToString("MM/dd/yyyy")}, ";
            if (Stats != null)
            {
                outLine += $"{Stats.Max}, "; // dataDateMax
                outLine += $"{Stats.P90}, "; // dataDateP90
                outLine += $"{Stats.P75}, "; // dataDateP75
                outLine += $"{Stats.P50}, "; // dataDateP50
                outLine += $"{Stats.P25}, "; // dataDateP25
                outLine += $"{Stats.P10}, "; // dataDateP10
                outLine += $"{Stats.Min}, "; // dataDateMin
                outLine += $"{Stats.Avg}, "; // dataDateAvg
                outLine += $"{Convert.ToDouble(dataValue.Value) / Stats.P50}, "; // dataDatePctMdn
                outLine += $"{Convert.ToDouble(dataValue.Value) / Stats.Avg}, "; // dataDatePctAvg
            }
            else
            {
                outLine += $", "; // dataDateMax
                outLine += $", "; // dataDateP90
                outLine += $", "; // dataDateP75
                outLine += $", "; // dataDateP50
                outLine += $", "; // dataDateP25
                outLine += $", "; // dataDateP10
                outLine += $", "; // dataDateMin
                outLine += $", "; // dataDateAvg
                outLine += $", "; // dataDatePctMdn
                outLine += $", "; // dataDatePctAvg
            }
            outLine += "9120, "; // statsPeriod - was hardcoded in the old version
            outLine += $"{MaxCapacity}, ";
            outLine += $"{Convert.ToDouble(dataValue.Value) / MaxCapacity}, "; // pctFull
            outLine += $"{TeacupName}, ";
            outLine += $"{DataUrl}, ";
            outLine += $"{Comment} ";
            return outLine;
        }

        public void GetStatsFromCsv(string csvPath)
        {
            List<string> lines = new List<string>();
            using (var reader = new StreamReader(csvPath))
            {
                while (!reader.EndOfStream)
                {
                    lines.Add(reader.ReadLine());
                }
                lines.RemoveAt(0);
            }

            foreach (string line in lines)
            {
                string[] values = line.Split(',');
                if (values[0] == SiteName)
                {
                    Stats stats = new Stats
                    {
                        Max = Convert.ToDouble(values[1]),
                        P90 = Convert.ToDouble(values[2]),
                        P75 = Convert.ToDouble(values[3]),
                        P50 = Convert.ToDouble(values[4]),
                        P25 = Convert.ToDouble(values[5]),
                        P10 = Convert.ToDouble(values[6]),
                        Min = Convert.ToDouble(values[7]),
                        Avg = Convert.ToDouble(values[8])
                    };
                    Stats = stats;
                }
            }
        }
    }

    public class Stats
    { 
        public double Max { get; set; }
        public double Min { get; set; }
        public double Avg { get; set; }
        public double P90 { get; set; }
        public double P75 { get; set; }
        public double P50 { get; set; }
        public double P25 { get; set; }
        public double P10 { get; set; }
    }
}
