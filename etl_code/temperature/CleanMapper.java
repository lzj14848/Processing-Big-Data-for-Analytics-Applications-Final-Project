
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
        extends Mapper<LongWritable, Text, Text,IntWritable> {
    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String s = value.toString();
        String seperate[] = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        
        if(seperate[6].indexOf("SOD") >= 0){
            String name = seperate[1] + "," + seperate[26] + "," + seperate[27] + "," + seperate[31] + "," + seperate[33] + "," + seperate[34] + "," + seperate[35] + "," + seperate[36] + "," + seperate[37];
            context.write(new Text(name), new IntWritable(3));}


    }
}



