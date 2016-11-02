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

  public class FineFoodReviews {

    public static class SimpleMapper extends Mapper<Object, Text, Text, Text>{

      static enum CountersEnum { INPUT_WORDS }

      
      private Text userId = new Text();
      private DoubleWritable helpful;
      private Text helpfulValue = new Text(); 
      private Configuration conf;
      private BufferedReader fis;

      @Override
      public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       Double a=0.0;
       Double b=0.0;
       String line = value.toString();
       String [] split = line.split(",");
       if(split[2].trim() == "UserId"){
        return;
      }
      else{
        userId.set(split[2].trim());
        try{		
         a = Double.parseDouble(split[4].trim());
         b = Double.parseDouble(split[5].trim());
         if(b==0.0 || b < a){
          return;	
        }
      }
      catch(Exception e){
  			//System.err.println("Ilegal input");
  			    // Discard input or request new input ...
  			    // clean up if necessary
       return;
     }  
     helpful = new DoubleWritable(a/b);
     helpfulValue = new Text(helpful.toString()); 
     context.write(userId,helpfulValue);
     Counter counter = context.getCounter(CountersEnum.class.getName(),
      CountersEnum.INPUT_WORDS.toString());
     counter.increment(1);
   }
 }
}

public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
  private Text result = new Text();
  private Text value1 = new Text();	 
  Double average = 0.0; 
  Double count = 0.0; 
  Double sum = 0.0; 
  public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException { 
    count = 0.0;
    sum = 0.0;      
    for (Text val : values) { 
      sum += Double.parseDouble(val.toString()); 
      count += 1.0;
    }
    average = sum/count;
    value1.set(average.toString()); 
    result.set(key+"_"+value1); 
    context.write(new Text("max"), result); 
  } 
}

public static class SimpleReducer extends Reducer<Text,Text,Text,DoubleWritable> { 
 private DoubleWritable max = new DoubleWritable(); 
 Double maxNum = 0.0;
 private Text userkey=new Text();
 String a="";
 String b="";
 Double x = 0.0;
 String userid = "";
 public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException { 		

  for (Text val : values) { 
    b = val.toString();
    userid = b.split("_")[0];  		
    x = Double.parseDouble(b.split("_")[1]);
    if(x > maxNum){
     userkey.set(userid);	
     maxNum = x;
   } 
   max.set(maxNum); 
 }
 context.write(userkey,max);
 System.out.println("UserKey:"+userkey+" Max:"+max);  

}
}

public static void main(String[] args) throws Exception {
  Configuration conf = new Configuration();
  GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
  String[] remainingArgs = optionParser.getRemainingArgs();
  Job job = Job.getInstance(conf, "word count");
  job.setJarByClass(FineFoodReviews.class);
  job.setMapperClass(SimpleMapper.class);
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(Text.class);
  job.setCombinerClass(IntSumReducer.class);
  job.setReducerClass(SimpleReducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(DoubleWritable.class);

  List<String> otherArgs = new ArrayList<String>();
  for (int i=0; i < remainingArgs.length; ++i) {
    if ("-skip".equals(remainingArgs[i])) {
      //code cleanup
    } else {
      otherArgs.add(remainingArgs[i]);
    }
  }
  FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
  FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

  System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
