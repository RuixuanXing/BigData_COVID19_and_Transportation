import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text count = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(value.toString().indexOf("C/A,UNIT") == -1) {
            String rawRecord = value.toString();
            String[] rawRecordArr = rawRecord.split(",");
            
            String SCP = rawRecordArr[2].trim();
            String STATION = rawRecordArr[3].trim();
            String LINENAME = rawRecordArr[4].trim();

            String DATE = rawRecordArr[6].trim();
            String[] dateArr = DATE.split("/");
            String DATE_MM = dateArr[0].trim();
            String DATE_DD = dateArr[1].trim();
            String DATE_YY = dateArr[2].trim();

            String TIME = rawRecordArr[7].trim();
            int ENTRIES = Integer.parseInt(rawRecordArr[9].trim());
            int EXITS = Integer.parseInt(rawRecordArr[10].trim());
            
            count.set(SCP + "," + STATION + "," + LINENAME + "," + DATE_YY + "-" + DATE_MM + "-" + DATE_DD + "," + TIME + "," + ENTRIES + "," + EXITS);
            context.write(count, one);
        }
    }
}