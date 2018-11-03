package MapReduce.MaxClicks;



import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxClicks {
	public static class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String rawWord, cleanWord;
			//Set<String> Stopwords = getStopWords();
		
			StringTokenizer st = new StringTokenizer(value.toString());
			//Text wordOut = new Text(1);
			//int id = Integer.parseInt(s[2])
			IntWritable one = new IntWritable(1);

			while(st.hasMoreTokens()) {
				rawWord = st.nextToken().toString();
				String[] s = rawWord.split(",");
				String[] date = s[1].split("-");
				if(date[1].equals("04"))
				{
					Text t = new Text(s[2]);
			
					
					context.write(t, one);
				}
			}
		}

	}

	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private TreeMap<Integer, ArrayList<String>> WordCountMap = new TreeMap<Integer, ArrayList<String>>();
		public void reduce(Text term, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {
			int count = 0;
			Iterator<IntWritable> iterator = ones.iterator();
			while(iterator.hasNext()) {
				count++;
				iterator.next();
			}
			if(WordCountMap.size()<10) {
				if(WordCountMap.get(count)==null) {
					WordCountMap.put(count, new ArrayList<String>());
					WordCountMap.get(count).add(term.toString());
				}
				else {
					WordCountMap.get(count).add(term.toString());
				}
			}
			else {
				if(WordCountMap.get(count)==null) {
					if(WordCountMap.firstKey()<count) {
						WordCountMap.pollFirstEntry();
						WordCountMap.put(count, new ArrayList<String>());
						WordCountMap.get(count).add(term.toString());
					}
				}
				else {
					WordCountMap.get(count).add(term.toString());
				}
			}
		
		}
		
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			int count = 10;
			while(!WordCountMap.isEmpty()){
				Map.Entry<Integer,ArrayList<String>> highest_freq = WordCountMap.pollLastEntry();
				Integer key = highest_freq.getKey();
				ArrayList<String> str = highest_freq.getValue();
				Collections.sort(str);
				for(String id:str) 
				{
					if(count>0) 
					{
						context.write(new Text(id.toString()), new IntWritable(key));
						count--;
//						IntWritable output = new IntWritable(count);
//						context.write(term, output);
					}
					else {
						break;
					}
				}
				if(count<0) {
					break;
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		if(otherArgs.length!=2) {
			System.err.println("Usage: WordCounter <input_file> <output_directory");
			System.exit(2); 
		}

		Job job = Job.getInstance(conf,"Word Count");
		job.setJarByClass(MaxClicks.class);
		job.setMapperClass(TokenizeMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean status = job.waitForCompletion(true);
		if(status) {
			System.exit(0);
		}
		else {
			System.exit(1);
		}
	}

}
