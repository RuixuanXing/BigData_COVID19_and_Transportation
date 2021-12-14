import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CleanRecsMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(","); 
        // tokens[1] := pickup date-time
        // tokens[3] := pickup location

        if(tokens.length == 7){
            // trim the pickup date-time only
            tokens[1] = tokens[1].trim(); 
            
            // remove the header and format the date time: [date-time], [1]
            if(tokens[1].length() == 19){
                context.write(new Text(tokens[1].substring(0, 10)), new IntWritable(1));
            }
        }
         
    }
}