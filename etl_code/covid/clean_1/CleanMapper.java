import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CleanMapper
	extends Mapper<Object, Text, Text, IntWritable>{
     
       private final static IntWritable one = new IntWritable(1);
       private Text word = new Text();

       public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

	  String[] line = value.toString().split(",");
	  String[] date = line[0].split("/");

	  if(line.length==6 && !(line[0].equals("TestDate")))
	  {
	  	Text content = new Text (date[2]+ "-"+ date[0]+ "-" + date[1] + "," +line[1]+","+line[2]);
	  	word.set(content);
	  	context.write(word, one);	
	  } 
}	
}
