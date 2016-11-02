import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class ActiveUser {

  public static class SimpleMapper extends Mapper<Object, Text, Text, Text>{

    private Text userId=new Text();
	
    private Configuration conf;
    private BufferedReader fis;
    
    private Text one=new Text(1+"");
	

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String line = value.toString();
    	String [] split = line.split(",");
        if(split[1].trim().equals("ProductId")){
			return;
		}	    
		Text userId=new Text();
		userId.set(split[2].trim());
	    context.write(userId,one);		
    }
  }


  public static class IntSumReducer extends Reducer<Text,Text,Text,Text> { 
  	private Text result = new Text();  
  	Integer count = 0; 
  	private Text commonKey=new Text("Max");
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException { 
		count = 0;  		
		for (Text val : values) {  
  			count += Integer.parseInt(val.toString());
		} 
		result.set(key+"_"+count); 
		context.write(commonKey, result); 
	} 
}


 public static class SimpleReducer extends Reducer<Text,Text,Text,IntWritable> { 
  	private IntWritable result = new IntWritable();  
  	Integer max = 0;
    Text maxUserkey=new Text(); 	
    Integer count=0;  	
public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException { 
		count = 0;  		
		Integer max=0;
		String str,user;
		String[] splitArr=null;
		for (Text val : values) {  
  			str=val.toString();
			
                       //System.out.println("Key="+key.toString()+" Str="+str);
                        splitArr=str.split("_");
			user=splitArr[0].trim();
			count=Integer.parseInt(splitArr[1].trim());
		       	
                       if(count>max){
			  max=count;
			  maxUserkey.set(user);
			}
		} 
		
		result.set(max); 
		context.write(maxUserkey, result); 
	} 
}

 


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(ActiveUser.class);
    job.setMapperClass(SimpleMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(SimpleReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        //job.addCacheFile(new Path(remainingArgs[++i]).toUri());
        //job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
