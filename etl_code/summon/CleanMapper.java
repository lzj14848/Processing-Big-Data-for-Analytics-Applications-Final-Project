import java.io.IOException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    if(value.toString().length()>0){
      // column selected: 0 key, 1 dates, 2 offense, 6 agegroup, 7 sex, 8 race, 10 BORO, 16 lon,lat
      String[] line = value.toString().split(",");
      if(line.length == 17){
        String[] selectedCol = {line[0],line[1],line[2],line[6],line[7],line[8],line[10],line[16]};
        context.write(new Text(String.join(",",selectedCol)), new IntWritable(1));
      }
    }
  }
}
      