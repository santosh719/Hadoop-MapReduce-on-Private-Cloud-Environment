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

public class ErrorPos {

  public static class SimpleMapper extends Mapper<Object, Text, Text, DoubleWritable>{

    static enum CountersEnum { INPUT_WORDS }

    //private final static IntWritable one = new IntWritable(1);
    private Text pos = new Text();
    private DoubleWritable err; 
//    private boolean caseSensitive;
//    private Set<String> patternsToSkip = new HashSet<String>();

    private Configuration conf;
    private BufferedReader fis;

//    @Override
//    public void setup(Context context) throws IOException, InterruptedException {
//      conf = context.getConfiguration();
//      caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
//      if (conf.getBoolean("wordcount.skip.patterns", true)) {
//        URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
//        for (URI patternsURI : patternsURIs) {
//          Path patternsPath = new Path(patternsURI.getPath());
//          String patternsFileName = patternsPath.getName().toString();
//          parseSkipFile(patternsFileName);
//        }
//      }
//    }

//    private void parseSkipFile(String fileName) {
//      try {
//        fis = new BufferedReader(new FileReader(fileName));
//        String pattern = null;
//        while ((pattern = fis.readLine()) != null) {
//          patternsToSkip.add(pattern);
//        }
//      } catch (IOException ioe) {
//        System.err.println("Caught exception while parsing the cached file '"
//            + StringUtils.stringifyException(ioe));
//      }
//    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // String line = (caseSensitive) ? value.toString() : value.toString().toLowerCase();
      // for (String pattern : patternsToSkip) {
      //   line = line.replaceAll(pattern, "");
      // }
      // StringTokenizer itr = new StringTokenizer(line);
      // while (itr.hasMoreTokens()) {
      //   word.set(itr.nextToken());
      //   context.write(word, one);
      //   Counter counter = context.getCounter(CountersEnum.class.getName(),
      //       CountersEnum.INPUT_WORDS.toString());
      //   counter.increment(1);
      // }
	double a=0.0;
    	String line = value.toString();
    	String [] split = line.split(",");
	if(split[5].trim() == "pos"){
		return;
	}
	else{
	    	pos.set(split[5].trim());
		//System.out.println("pos = "+split[5].trim());
		try{
				
			//System.out.println("err = "+split[11].trim());		
			a = Double.parseDouble(split[11].trim());
		}
		catch(Exception e){
			//System.err.println("Ilegal input");
			    // Discard input or request new input ...
			    // clean up if necessary
			return;
		}  
		err = new DoubleWritable(a);
	    	context.write(pos,err);
	    	Counter counter = context.getCounter(CountersEnum.class.getName(),
	    	CountersEnum.INPUT_WORDS.toString());
	    	counter.increment(1);
	}
    }
  }

  // public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
  //   private IntWritable result = new IntWritable();

  //   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
  //     int sum = 0;
  //     for (IntWritable val : values) {
  //       sum += val.get();
  //     }
  //     result.set(sum);
  //     context.write(key, result);
  //   }
  // }

  public static class SimpleReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> { 
  	private DoubleWritable result = new DoubleWritable(); 
  	Double average = 0.0; 
  	Double count = 0.0; 
  	Double sum = 0.0; 
  	public void reduce(Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException { 
		count = 0.0;
		sum = 0.0;  		
		for (DoubleWritable val : values) { 
  			sum += val.get(); 
  			count += 1.0;
		}
		System.out.println("key = " + key + " count = " + count + " sum " + sum);  
		average = sum/count; 
		result.set(average); 
		context.write(key, result); 
	} 
}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    // if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
    //   System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
    //   System.exit(2);
    // }
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(ErrorPos.class);
    job.setMapperClass(SimpleMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(SimpleReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

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
