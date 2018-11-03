package wordCount2000.wordCount2000;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class wordCount2000 {
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
			StringTokenizer st = new StringTokenizer(value.toString());
			String[] stopwords = {"a","about","above","after" ,"again" ,"against" ,"all" ,"am" ,"an" ,
			             "and" ,"any" ,"are" ,"aren't" ,"as" ,"at" ,"be" ,"because" ,"been" ,"before" ,
			             "being" ,"below" ,"between" ,"both" ,"but" ,"by" ,"can't" ,"cannot" ,"could" ,
			             "couldn't" ,"did" ,"didn't" ,"do" ,"does" ,"doesn't" ,"doing" ,"don't" ,"down" ,
			             "during" ,"each" ,"few" ,"for" ,"from" ,"further" ,"had" ,"hadn't" ,"has" ,"hasn't" ,
			             "have" ,"haven't" ,"having" ,"he" ,"he'd" ,"he'll" ,"he's" ,"her" ,"here" ,"here's" ,
			             "hers" ,"herself" ,"him" ,"himself" ,"his" ,"how" ,"how's" ,"i" ,"i'd" ,"i'll" ,"i'm" ,
			             "i've" ,"if" ,"in" ,"into" ,"is" ,"isn't" ,"it" ,"it's" ,"its" ,"itself" ,"let's" ,"me" ,
			             "more" ,"most" ,"mustn't" ,"my" ,"myself" ,"no" ,"nor" ,"not" ,"of" ,"off" ,"on" ,"once" ,
			             "only" ,"or" ,"other" ,"ought" ,"our" ,"ours" ,"ourselves" ,"out" ,"over" ,"own" ,"same" ,
			             "shan't" ,"she" ,"she'd" ,"she'll" ,"she's" ,"should" ,"shouldn't" ,"so" ,"some" ,"such" ,"than" ,
			             "that" ,"that's" ,"the" ,"their" ,"theirs" ,"them" ,"themselves" ,"then" ,"there" ,"there's" ,"these" ,
			             "they" ,"they'd" ,"they'll" ,"they're" ,"they've" ,"this" ,"those" ,"through" ,"to" ,"too" ,"under" ,"until" ,
			             "up" ,"very" ,"was" ,"wasn't" ,"we" ,"we'd" ,"we'll" ,"we're" ,"we've" ,"were" ,"weren't" ,"what" ,
			             "what's" ,"when" ,"when's" ,"where" ,"where's" ,"which" ,"while" ,"who" ,"who's" ,"whom" ,"why" ,"why's" ,
			             "with" ,"won't" ,"would" ,"wouldn't" ,"you" ,"you'd" ,"you'll" ,"you're" ,"you've" ,"your" ,"yours" ,
			             "yourself" ,"yourselves"};
			Text wordOut = new Text();
			IntWritable one = new IntWritable(1);
			while(st.hasMoreTokens()) {
				wordOut.set(st.nextToken());
				String s = wordOut.toString().replaceAll("[^a-zA-Z']", "").toLowerCase();
			        if(s.length() != 0 && ArrayUtils.contains(stopwords, s)==false && s != "") {
			        	String temp = s.replaceAll("[']", "");
			        	if(temp.equals("") || ArrayUtils.contains(stopwords, temp)==true);
			        	else {
			        		wordOut.set(temp);
			        		context.write(wordOut, one);}
			        	}
			}

		}
	}
	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private Map countMap = new HashMap<>();
		public void reduce(Text term, Iterable<IntWritable> ones, Context context) throws IOException, InterruptedException{
			int count = 0;
			Iterator<IntWritable> iterator = ones.iterator();
			while(iterator.hasNext()) {
				count++;
				iterator.next();
			}
			IntWritable output = new IntWritable(count);
			if(count > 10)
				countMap.put(new Text(term), new IntWritable(count));
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			Map<Text, IntWritable> sortedMap = sortByValues(countMap);
			
			Set<Entry<Text, IntWritable>> set = sortedMap.entrySet();
            List<Entry<Text, IntWritable>> list = new ArrayList<Entry<Text, IntWritable>>(set);
            Collections.sort( list, new Comparator<Map.Entry<Text, IntWritable>>() {
              public int compare( Map.Entry<Text, IntWritable> o1, Map.Entry<Text, IntWritable> o2 )
              {
                int result = (o2.getValue()).compareTo( o1.getValue() );
                if (result != 0) {
                  return result;
                } else {
                  return o1.getKey().compareTo(o2.getKey());
                }
              }
            } );
			
			int counter = 0;
			for(Entry<Text, IntWritable> term:list) {
				if(counter++ == 2000) 
					break;
				context.write(term.getKey(), term.getValue());
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(); 
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 2) {
			System.err.println("Usage: WordCount <input_file> <output_directory>");
			System.exit(3);
		}
		
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(wordCount2000.class);
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

