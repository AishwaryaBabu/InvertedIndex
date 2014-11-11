package edu.rutgers.cloud;

import java.io.IOException;

import java.io.StringReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

/**
 * Inverted index- Map Reduce program
 * @author Saumya
 *
 */
public class Main{

	
	/**
	 * The Reducer class
	 * @author Saumya
	 *
	 */
	public static class SearchReducer extends Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterable<Text> values, Context ctx) 
				throws IOException, InterruptedException {
			
			//Hashmap to save <filename,count> , where count is number of times the word occured in the file
			HashMap<String, Integer> index=new HashMap<String, Integer>();
			for (Text file: values){
				if(index.containsKey(file.toString())){
					index.put(file.toString(), index.get(file.toString())+1);
				}
				else{
					index.put(file.toString(),new Integer(1));
				}
			}
			
			//Reducer output <word,{<filename,count>}> where <filename,count> map is sorted by count
			ctx.write(key, new Text(sortByValues(index).toString()));

		}
	}

	/**
	 * The Mapper class
	 * @author Saumya
	 *
	 */
	public static class SearchMapper  extends Mapper<LongWritable, Text, Text, Text> {

		static boolean text=false; static String location=""; static int noOfWords=0;
		@Override
		public void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {			
			
			//Analyzer which tokenizes,removes stop words and stems words
			EnglishAnalyzer stopWords = new EnglishAnalyzer(Version.LUCENE_36);
			TokenStream analyzedTokens=stopWords.tokenStream(null, new StringReader(value.toString().toLowerCase()));
			
			//To get file name
			FileSplit fileSplit = (FileSplit)ctx.getInputSplit();
			Text filePath = new Text(fileSplit.getPath().getName());
			
			//To read the analyzed tokens
			CharTermAttribute cattr = analyzedTokens.addAttribute(CharTermAttribute.class);
			analyzedTokens.reset();
			
			//To get contents after Parsetext
			if(!location.equals(filePath.toString())){
				text=false;
				noOfWords=0;
			}
			location=filePath.toString();
			
			//Iterate through the tokens and consider ones after parsetext
			while (analyzedTokens.incrementToken()) {
				if(text){
					noOfWords++; //gives total number of words
					//mapper emits <word,filename>
					ctx.write(new Text(cattr.toString()), filePath);
				}
				if(cattr.toString().equals("parsetext"))
				{
					text=true;
				}

			}
			analyzedTokens.end();
			analyzedTokens.close();
			stopWords.close();
		}		
	}

	public static void main(String[] args) throws Exception {
		
		//If more than two arguments, throw exception
		if(args.length <2){
			throw new Exception("Usage <input directory> <output directory>");
		}
		Configuration conf = new Configuration();
		
		//Create a mapreduce job
		Job job = new Job(conf);

		//Set input and output key class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//Set Mapper and reducer class
		job.setMapperClass(Main.SearchMapper.class);
		job.setReducerClass(Main.SearchReducer.class);
		
		//Set input and output format class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Set input and output file path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Set the driver class
		job.setJarByClass(Main.class);
		job.waitForCompletion(true);
	}
	
	/**
	 * Method to sort hashmap by value
	 * @param map
	 * @return map
	 */
	  public static <K extends Comparable,V extends Comparable> Map<K,V> sortByValues(Map<K,V> map){
	        List<Map.Entry<K,V>> entries = new LinkedList<Map.Entry<K,V>>(map.entrySet());
	      
	        Collections.sort(entries, new Comparator<Map.Entry<K,V>>() {
	        	//overrides the compare function
	            @Override
	            public int compare(Entry<K, V> o1, Entry<K, V> o2) {
	            	//sorts by descending order
	                return o2.getValue().compareTo(o1.getValue());
	            }
	        });
	        
	        //Sorted map
	        Map<K,V> sortedMap = new LinkedHashMap<K,V>();	      
	        for(Map.Entry<K,V> entry: entries){
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }
	      
	        return sortedMap;
	    }
}