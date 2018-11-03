package successRatePartC.successRatePartC;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class successRate {
	public static <K extends Comparable, V extends Comparable> Map<K,V> sortByValues(Map<K,V> map){
		List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
		
		Collections.sort(entries, new Comparator<Map.Entry<K, V>>(){
			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2){
				return o2.getValue().compareTo(o1.getValue());
			}
		});
		
		Map<K, V> sortedMap = new LinkedHashMap<K, V>();
		
		for(Map.Entry<K, V> entry: entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
	
	public static class TokenizeMapper1 extends Mapper<Object, Text, Text, FloatWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] buysDetails = value.toString().split(",");
			Text wordOut = new Text(buysDetails[2]);
			FloatWritable buy = new FloatWritable(1);
			context.write(wordOut, buy);
		}
	}
	
	public static class TokenizeMapper2 extends Mapper<Object, Text, Text, FloatWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] clicksDetails = value.toString().split(",");
			Text wordOut = new Text(clicksDetails[2]);
			FloatWritable click = new FloatWritable(0);
			context.write(wordOut, click);
		}
	}
		
	public static class SumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
		private Map<Text, FloatWritable> countMap = new HashMap<Text, FloatWritable>();
		public void reduce(Text id, Iterable<FloatWritable> buysOrClicks, Context context) throws IOException, InterruptedException {
			float buysCount = 0;
			float clicksCount = 0;
			for (FloatWritable text : buysOrClicks) {
				if(text.equals(new FloatWritable(1))) {buysCount++;}
				if(text.equals(new FloatWritable(0))) {clicksCount++;}
			}
			float successRate =  (float)(buysCount)/(float)clicksCount;
			countMap.put(new Text(id), new FloatWritable(successRate));	

		}
			
		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<Text, FloatWritable> sortedMap = sortByValues(countMap);
			int counter = 0;
			for(Text term:sortedMap.keySet()) {
				if(counter++ == 10) 
					break;
				context.write(term, sortedMap.get(term));
			}
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length!=3) {
			System.err.println("Usage: buyClick <buys.txt <clicks.txt <output_directory");
			System.exit(2);
		}
			Job job = Job.getInstance(conf,"Buy Click");
			job.setJarByClass(successRate.class);
			job.setMapperClass(TokenizeMapper1.class);
			job.setMapperClass(TokenizeMapper2.class);
			job.setReducerClass(SumReducer.class);
			job.setNumReduceTasks(1);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			
			MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TokenizeMapper1.class);
			MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, TokenizeMapper2.class);
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
			boolean status = job.waitForCompletion(true);
			if(status) {
				System.exit(0);
			}
			else {
				System.exit(1);
			}
	}
}
