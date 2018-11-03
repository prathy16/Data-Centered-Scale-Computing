package Mapreduce.MaxBuys;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
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

public class MaxBuys {
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
	public static class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable>{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String rawWord, cleanWord;
		
			StringTokenizer st = new StringTokenizer(value.toString());
			//

			while(st.hasMoreTokens()) {
				rawWord = st.nextToken().toString();
				String[] s = rawWord.split(",");
				String[] date = s[1].split("T");
				String time = date[1].substring(0, 2);
				//System.out.println(time);
				String price = s[3];
				String qty = s[4];
				int price1 = Integer.parseInt(price);
				int quantity = Integer.parseInt(qty);
				int rev = price1 * quantity;
				//Text t = new Text(s[2]);
				Text word = new Text(time);
				IntWritable one = new IntWritable(rev);
				context.write(word, one);
			}
		}

	}

	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private Map countMap = new HashMap<>();
		private TreeMap<Integer, ArrayList<String>> WordCountMap = new TreeMap<Integer, ArrayList<String>>();
		public void reduce(Text term, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException {
			int count = 0;
			Iterator<IntWritable> iterator = ones.iterator();
			while(iterator.hasNext()) {
				count += iterator.next().get();
			}
			IntWritable output = new IntWritable(count);
			countMap.put(new Text(term), new IntWritable(count));
			
		}
		
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			Map<Text, IntWritable> sortedMap = sortByValues(countMap);
			int counter = 0;
			for(Text term:sortedMap.keySet()) {
//				if(counter ++ == 2000) 
//					break;
				context.write(term, sortedMap.get(term));
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
		job.setJarByClass(MaxBuys.class);
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