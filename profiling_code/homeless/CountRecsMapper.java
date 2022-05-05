import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRecsMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split(",");
        if(line.length >= 2 ) {
            String text = "Total number of records:";
            context.write(new Text(text), new IntWritable(1));
		}
		
    }
}

