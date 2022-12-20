import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] tokens = line.split(",");
        int[] indices = {9, 14};
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 2; i++)
        {
            sb.append(tokens[indices[i]] + ",");
        }
        sb.deleteCharAt(sb.length() -1);
        String cleaned = sb.toString();
        context.write(new Text(cleaned), new Text(""));
    }
}
