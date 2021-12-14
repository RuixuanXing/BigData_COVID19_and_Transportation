import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.mapreduce.Reducer;

public class CleanRecsReducer
    extends Reducer<Text, IntWritable, Text, NullWritable> {
    
    private NullWritable result = NullWritable.get(); 
    
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // [date-time], [values]
        // output total number of trips with key (date-time, location)
        int count = 0; 
        for (IntWritable value : values) {
            ++count; 
        }
        String date = key.toString(); 

        context.write(new Text(date + "," + String.valueOf(count)), result); 
        
    }
}
