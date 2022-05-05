import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text count = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String rawLine = value.toString();
        String[] rawRecArr = rawLine.split(",");
        
        if(!rawRecArr[0].equals("Unique Key")) {
            String Created_Date = rawRecArr[1];
            String Closed_Date = rawRecArr[2];
            String Incident_Addr = rawRecArr[9];
            String city = rawRecArr[16];
            String Resolution_Date = rawRecArr[22];
            String Borough = rawRecArr[24];

            
            count.set(Created_Date + "," + Closed_Date + "," + Incident_Addr + "," + city + "," + Resolution_Date + "," + Borough);
            context.write(count, new IntWritable(1));
        }
        
        
        
        
    }
}
