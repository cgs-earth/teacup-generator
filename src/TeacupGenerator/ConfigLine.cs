using System;

namespace Teacup
{
    //This class looks into the type of config line that is being read and responds accordingly
    class ConfigLine
    {
        string line;
        public Int32 row = 0;
        public Int32 col = 0;
        public Int32 size = 0;
        public int sdi = 0;
        public int sdi2 = 0;
        public double capacity = 0;
        public string ResName = "";
        public string DisplayName = "";
        public string type = "";
        public string units = "";
        public string output = "";

        public ConfigLine(string line)
        {
            this.line = line;
            //line = line.Replace("\t", ",");
            string[] x = System.Text.RegularExpressions.Regex.Split(line.Trim(), @",");

            output = x[0].Trim();
            type = x[1].Trim();
            col = Convert.ToInt32(x[2].Trim()); //x
            row = Convert.ToInt32(x[3].Trim()); //y
            
            if (IsTeacup)
            {
                size = Convert.ToInt32(x[6].Trim());
                capacity = Convert.ToDouble(x[7].Trim());
                ResName = x[8].Trim();
                DisplayName = x[9].Trim();
            }

            if (IsCFS)
            {
                ResName = x[5].Trim();
            }
            
        }

        public bool IsTeacup
        {
            get
            { return type == "teacup"; }
        }
        
        public bool IsCFS
        {
            get
            { return type == "cfs"; }
        }
        
        

    }
}
